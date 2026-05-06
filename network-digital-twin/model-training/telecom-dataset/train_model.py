import argparse
import pandas as pd
import numpy as np
import os
import tensorflow as tf
from sklearn.metrics import mean_squared_error, r2_score, mean_absolute_percentage_error
from sklearn.preprocessing import MinMaxScaler
import pickle as pkl

from tensorflow.keras.optimizers import Adam
from tensorflow.keras.layers import Input, Dense, Reshape, LSTM,GRU,SimpleRNN
from tensorflow.keras.callbacks import EarlyStopping
from tensorflow.keras import Model

import matplotlib.pyplot as plt


def create_sequence(data, time_steps, horizon_steps):
    X, y = [], []
    for i in range(len(data) - time_steps - horizon_steps):
        X.append(np.array(data[i:i+time_steps]))
        y.append(np.array(data[i+time_steps:i+time_steps+horizon_steps]))
    return np.array(X), np.array(y)

def load_and_preprocess_data(data_path, cell_id, features, time_steps, horizon_steps, train_perc=0.8, val_perc=0.9, save_output=False):
    """Loads dataset, scales it, and creates sequences for training/testing."""
    file_path = os.path.join(data_path, f"traffic-aggr-{cell_id}.csv")
    df = pd.read_csv(file_path, index_col=0)
    
    # Split train, val, test
    n_train = int(len(df) * train_perc)
    n_val = int(len(df) * val_perc)
    
    df_train = df.iloc[:n_train]
    df_val = df.iloc[n_train:n_val]
    df_test = df.iloc[n_val:]

    if save_output:
        df_train.to_csv(os.path.join("training-data",f"train-{cell_id}.csv"),index=False)
        df_val.to_csv(os.path.join("training-data",f"val-{cell_id}.csv"),index=False)
        df_test.to_csv(os.path.join("training-data",f"test-{cell_id}.csv"),index=False)
    
    # Scale data
    scaler = MinMaxScaler()
    scaler.fit(df_train[features])


    if save_output:
        with open(os.path.join("models","scaler.pkl"), 'wb') as file:
            pkl.dump(scaler, file)
    
    df_train_scaled = pd.DataFrame(scaler.transform(df_train[features]), columns=features)
    df_val_scaled = pd.DataFrame(scaler.transform(df_val[features]), columns=features)
    df_test_scaled = pd.DataFrame(scaler.transform(df_test[features]), columns=features)
    
    X_train, y_train = create_sequence(df_train_scaled.values, time_steps, horizon_steps)
    X_val, y_val = create_sequence(df_val_scaled.values, time_steps, horizon_steps)
    X_test, y_test = create_sequence(df_test_scaled.values, time_steps, horizon_steps)
    
    return (X_train, y_train), (X_val, y_val), (X_test, y_test), scaler

def create_lstm_model(time_steps, n_features, horizon_steps,hidden_layers=1, hidden_neurons=64,dense_neurons=32,lr=0.0001):
    """Builds and compiles an LSTM model."""
    inputs = Input(shape=(time_steps, n_features))
    
    # Backbone
    for i in range(hidden_layers-1):
        if i==0:
            x = LSTM(hidden_neurons, return_sequences=True)(inputs)
        else:
            x = LSTM(hidden_neurons, return_sequences=True)(x)
    x = LSTM(hidden_neurons)(inputs) if hidden_layers==1 else LSTM(hidden_neurons)(x)
    x = Dense(dense_neurons,"relu")(x)
    
    # Output heads
    heads = []
    for i in range(n_features):
        x_head = Dense(horizon_steps, activation='linear', name=f"head_{i}")(x)
        heads.append(Reshape((horizon_steps, 1))(x_head))
        
    model = Model(inputs=inputs, outputs=heads)
    model.compile(optimizer=Adam(learning_rate=lr), loss='mse')
    return model

def create_gru_model(time_steps, n_features, horizon_steps,hidden_layers=1, hidden_neurons=64,dense_neurons=32,lr=0.0001):
    """Builds and compiles an LSTM model."""
    inputs = Input(shape=(time_steps, n_features))
    
    # Backbone
    for i in range(hidden_layers-1):
        if i ==0:
            x = GRU(hidden_neurons, return_sequences=True)(inputs)
        else:
            x = GRU(hidden_neurons, return_sequences=True)(x)
    x = GRU(hidden_neurons)(inputs) if hidden_layers==1 else GRU(hidden_neurons)(x)
    x = Dense(dense_neurons,"relu")(x)
    
    # Output heads
    heads = []
    for i in range(n_features):
        x_head = Dense(horizon_steps, activation='linear', name=f"head_{i}")(x)
        heads.append(Reshape((horizon_steps, 1))(x_head))
        
    model = Model(inputs=inputs, outputs=heads)
    model.compile(optimizer=Adam(learning_rate=lr), loss='mse')
    return model

def create_rnn_model(time_steps, n_features, horizon_steps,hidden_layers=1, hidden_neurons=64,dense_neurons=32,lr=0.0001):
    """Builds and compiles an LSTM model."""
    inputs = Input(shape=(time_steps, n_features))
    
    # Backbone
    for i in range(hidden_layers-1):
        if i == 0:
            x = SimpleRNN(hidden_neurons, return_sequences=True)(inputs)
        else:
            x = SimpleRNN(hidden_neurons, return_sequences=True)(x)
    x = SimpleRNN(hidden_neurons)(inputs) if hidden_layers==1 else SimpleRNN(hidden_neurons)(x)
    x = Dense(dense_neurons,"relu")(x)
    
    # Output heads
    heads = []
    for i in range(n_features):
        x_head = Dense(horizon_steps, activation='linear', name=f"head_{i}")(x)
        heads.append(Reshape((horizon_steps, 1))(x_head))
        
    model = Model(inputs=inputs, outputs=heads)
    model.compile(optimizer=Adam(learning_rate=lr), loss='mse')
    return model

def train_model(model, X_train, y_train, X_val, y_val, epochs=200, batch_size=32):
    """Trains the model with early stopping."""
    early_stopping = EarlyStopping(monitor='val_loss', patience=15, restore_best_weights=True)

    y_train_list = y_train
    y_val_list = y_val

    history = model.fit(
        X_train, y_train_list,
        validation_data=(X_val, y_val_list),
        epochs=epochs,
        batch_size=batch_size,
        callbacks=[early_stopping],
        verbose=1
    )
    return model, history

def main():
    parser = argparse.ArgumentParser(description="Train a time series forecasting model.")
    
    # Model parameters
    parser.add_argument('--models', type=str, nargs="+",default=['lstm','gru','rnn'], help="Model types to train")
    parser.add_argument('--hidden-layers', type=int, default=1, help="Number of hidden layers")
    parser.add_argument('--n-neurons', type=int, default=64, help="Number of neurons in the hidden layers")
    parser.add_argument('--n-dense_neurons', type=int, default=32, help="Number of neurons in the dense layer")
    parser.add_argument('--batch-size', type=int, default=32, help="Batch size for training")
    parser.add_argument('--epochs', type=int, default=200, help="Number of epochs to train")
    parser.add_argument('--learning-rate', type=float, default=0.0001, help="Learning rate")
    
    # Data parameters
    parser.add_argument('--data-path', type=str, default='clean-data', help="Path to data directory")
    parser.add_argument('--cell-id', type=int, default=5060, help="Cell ID for the dataset")
    parser.add_argument('--time-steps', type=int, default=6, help="Number of time steps (lookback)")
    parser.add_argument('--horizon-steps', type=int, default=6, help="Number of horizon steps (forecast)")
    
    args = parser.parse_args()
    
    print("=== Loading and preprocessing data ===")
    features = ["internet"] # Default from notebook
    (X_train, y_train), (X_val, y_val), (X_test, y_test), scaler = load_and_preprocess_data(
        args.data_path, args.cell_id, features, args.time_steps, args.horizon_steps, save_output=True
    )
    print(f"X_train shape: {X_train.shape}")
    print(f"y_train shape: {y_train.shape}")
    


    for m in args.models:
        print(f"=== Creating {m.upper()} model ===")
        if  m == 'lstm':
            model = create_lstm_model(args.time_steps, len(features), args.horizon_steps,args.hidden_layers, args.n_neurons,args.n_dense_neurons,args.learning_rate)
        elif m == 'gru':
            model = create_gru_model(args.time_steps, len(features), args.horizon_steps,args.hidden_layers, args.n_neurons,args.n_dense_neurons,args.learning_rate)
        elif m == 'rnn':
            model = create_rnn_model(args.time_steps, len(features), args.horizon_steps,args.hidden_layers, args.n_neurons,args.n_dense_neurons,args.learning_rate)
        else:
            raise ValueError(f"Model {m} is not implemented.")
            
        model.summary()
        
        print("=== Training model ===")
        model, history = train_model(model, X_train, y_train, X_val, y_val, args.epochs, args.batch_size)

        fig=plt.figure()
        plt.plot(history.history['loss'], label='Train Loss')
        plt.plot(history.history['val_loss'], label='Validation Loss')
        plt.xlabel('Epochs')
        plt.ylabel('Loss')
        plt.title('Training vs Validation Loss')
        plt.legend()
        fig.savefig(os.path.join("plots",f"{args.hidden_layers}l-{m}-train-val-loss-{args.cell_id}.png"))
        plt.close()
        print(f"Training History plot saved in {os.path.join("plots",f"{args.hidden_layers}l-{m}-train-val-loss-{args.cell_id}.png")}")
        print("=== Training completed ===")

        print("=== Saving Model ===")
        model.save(os.path.join("models",f"{args.hidden_layers}l-{m}-{args.cell_id}.keras"))
        print(f"Model saved in {os.path.join("models",f"{args.hidden_layers}l-{m}-{args.cell_id}.keras")}")
    
if __name__ == "__main__":
    main()