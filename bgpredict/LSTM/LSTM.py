from datetime import datetime
import matplotlib.pyplot as plt
import numpy as np
import os
import pandas as pd
from sklearn.preprocessing import MinMaxScaler
import sqlalchemy
from sqlalchemy import create_engine
import s3fs
import gc

from functools import partial
import ray
from ray import tune
from ray.tune import JupyterNotebookReporter
from ray.tune.schedulers import ASHAScheduler
from ray.tune.suggest.hyperopt import HyperOptSearch
from ray.tune.stopper import TrialPlateauStopper

import torch
import torch.optim as optim
import torch.nn as nn
import torch.nn.functional as F

from dotenv import load_dotenv

class Net(nn.Module):
    
    def __init__(self, input_size=30, hidden_size=8, num_lstm_layers=1, dropout=0):
        super().__init__()
        if num_lstm_layers == 1:
            dropout = 0
        self.lstm = nn.LSTM(input_size=input_size,
                            hidden_size=hidden_size,
                            num_layers=num_lstm_layers,
                            dropout=dropout, batch_first=True)
        self.fc1 = nn.Linear(hidden_size, 1)
        
    def forward(self, x):
        lstm_out, _ = self.lstm(x)
        x = self.fc1(lstm_out)
        return x
    
def train_lstm(config, train_X, train_y, val_X, val_y, checkpoint_dir=None,):
    
    device = "cpu"
    if torch.cuda.is_available():
        device = "cuda:0"
    
    print(f"DEVICE: {device}")
    
    # Configure the network and send it to the device
    # Width of the dataframe - 1 (y variable) is feature set size 
    input_size = train_X.shape[2]
    net = Net(input_size=input_size,
              hidden_size=config['hidden_size'],
              num_lstm_layers=config['num_lstm_layers'],
              dropout=config['dropout'])
    
    if torch.cuda.device_count() > 1:
        net = nn.DataParallel(net)     
    net.to(device)
    
    optimizer = optim.Adam(net.parameters(), lr=config['learning_rate'])
    optimizer
    
    train_X = train_X.to(device)
    train_y = train_y.to(device)
    val_X = val_X.to(device)
    val_y = val_y.to(device)
    
    # Checkpoint Dir Stuff -- handled by Tune 
    if checkpoint_dir:
        checkpoint = os.path.join(checkpoint_dir, "checkpoint")
        model_state, optimizer_state = torch.load(checkpoint)
        net.load_state_dict(model_state)
        optimizer.load_state_dict(optimizer_state)    
    
    # train
    running_loss = 0
    running_val_loss = 0
    BATCH_SIZE = config['batch_size']
    for epoch in range(1000):
        epoch_start = datetime.now()
        for i in range(0, len(train_X)-BATCH_SIZE, BATCH_SIZE):
            X = train_X[i:i+BATCH_SIZE]
            y = train_y[i:i+BATCH_SIZE]
            net.zero_grad()

            out_seq = net(X)
            first_dim, second_dim, _ = out_seq.shape
            pred = out_seq.view(first_dim, second_dim)[:, -1]
            loss = F.mse_loss(pred, y)
            
            loss.backward()
            optimizer.step()
            
            running_loss += loss.item() * BATCH_SIZE
        
        epoch_train_loss = running_loss / train_X.shape[0]
        
        # Compute Validation Loss
        with torch.no_grad():
            for i in range(0, len(val_X)-BATCH_SIZE, BATCH_SIZE):
                X = val_X[i:i+BATCH_SIZE]
                y = val_y[i:i+BATCH_SIZE]
                out_seq = net(X)
                first_dim, second_dim, _ = out_seq.shape
                pred = out_seq.view(first_dim, second_dim)[:, -1]
                loss = F.mse_loss(pred, y)
                running_val_loss += loss.item() * BATCH_SIZE
        
        epoch_val_loss = running_val_loss / val_X.shape[0]

        running_val_loss = 0 
        running_loss = 0
        
        # Tune Metrics
        with tune.checkpoint_dir(step=epoch) as checkpoint_dir:
            path = os.path.join(checkpoint_dir, "checkpoint")
            torch.save((net.state_dict(), optimizer.state_dict()), path)

            tune.report(val_loss=epoch_val_loss, train_loss=epoch_train_loss, training_iteration=epoch)
            print(f"Finished epoch {epoch} in {datetime.now()-epoch_start}")
    

        
        epoch+=1
    
    print("finished!")    
        
    
print(f"Cuda available: {torch.cuda.is_available()}")
    
s3 = s3fs.S3FileSystem()
string = "loading train X"
if os.path.exists("./train_X.pt"):
    print(f"{string}locally")
    with open("./train_X.pt", 'rb') as f:
        train_X = torch.load(f)
else:
    print(f"{string}remotely")
    with s3.open("s3://bgpredict/models/lstm/tensors/train_X", 'rb') as f:
        train_X=torch.load(f)
        
string = "loading train y "
if os.path.exists("./train_y.pt"):
    print(f"{string}locally")
    with open("./train_y.pt", "rb") as f:
        train_y = torch.load(f)
else:
    print(f"{string}remotely")
    with s3.open("s3://bgpredict/models/lstm/tensors/train_y", 'rb') as f:
        train_y=torch.load(f)

string = "loading val X"
if os.path.exists("./val_X.pt"):
    print(f"{string}locally")
    with open("./val_X.pt", 'rb') as f:
        val_X = torch.load(f)
else:
    print(f"{string}remotely")
    with s3.open("s3://bgpredict/models/lstm/tensors/val_X", 'rb') as f:
        val_X=torch.load(f) 

string = "loading val y"
if os.path.exists("./val_y.pt"):
    print(f"{string}locally")
    with open("./val_y.pt", 'rb') as f:
        val_y = torch.load(f)
else:
    print(f"{string}remotely")
    with s3.open("s3://bgpredict/models/lstm/tensors/val_y", 'rb') as f:
        val_y=torch.load(f)    
        

config= {
    'hidden_size': tune.choice([2**x for x in range(3, 9)]), # 2^3 to 2^7, 8 to 128
    'num_lstm_layers':tune.choice([1,2,3,4]),
    'dropout': tune.choice([0, 0.1, 0.2, 0.3, 0.4, 0.5]), # [0,0.5]
    'learning_rate': tune.choice([0.0001, 0.001, 0.01, 0.1]),
    'batch_size': tune.choice([64, 128, 256, 512, 1024, 2048])
}

initial_params = [{"hidden_size": 128,
                  "num_lstm_layers": 1,
                  "dropout": 0,
                  'learning_rate': 0.1,
                  "batch_size": 512}]

def stopper(trial_id, result):
    train_times = [10 * i for i in range(11)]
    training_iteration = result["training_iteration"]
    val_loss = result['val_loss']
    return True

def mse(prediction, target):
    loss = (prediction - target)**2
    ls = (loss.sum() / len(target))
    return ls

hyperopt_search = HyperOptSearch(metric='val_loss', mode='min', points_to_evaluate=initial_params)

scheduler = ASHAScheduler(
    metric="val_loss",
    mode="min",
    grace_period=3,
    max_t=200
)


if __name__ == "__main__":
    ray.shutdown()
    ray.init()
    result = tune.run(
    tune.with_parameters(
        train_lstm,
        train_X = train_X,
        train_y = train_y,
        val_X = val_X,
        val_y = val_y
    ),
    resources_per_trial={"cpu":10, "gpu":1},
    config=config,
    num_samples=100,
    search_alg=hyperopt_search,
    name='GPU_fixed',
    scheduler = scheduler,
    resume = False
    )
    best_trial = result.get_best_trial('val_loss', 'min', 'all')
    dfs = result.trial_dataframes
    ax = None  # This plots everything on the same plot
    for d in dfs.values():
        ax = d.val_loss.plot(ax=ax, legend=False)