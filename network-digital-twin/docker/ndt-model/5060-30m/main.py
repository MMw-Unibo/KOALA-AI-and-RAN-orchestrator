import inference_pb2
import inference_pb2_grpc
import os
from datetime import datetime,timedelta
import pickle
from tensorflow.keras.models import load_model
from influxdb_client_3 import InfluxDBClient3, write_client_options, SYNCHRONOUS
import grpc
from concurrent import futures

INFLUX_HOST=os.getenv("INFLUX_HOST")
INFLUX_DB=os.getenv("INFLUX_DB")

class MultiStepInferenceServicer(inference_pb2_grpc.InferenceServicer):
    def __init__(self):
        super().__init__()

        print("Loading scaler:",end=" ")
        self.scaler=pickle.load(open("scaler.pkl","rb"))
        print("Ok")

        print("Loading model:",end=" ")
        self.model=load_model("model.keras")
        self.window_size=self.model.input_shape[1];
        print("Ok")

        print("Creating InfluxDB client:",end=" ")
        wco = write_client_options(write_options=SYNCHRONOUS)
        self.influx_client = InfluxDBClient3(
            host=f"{INFLUX_HOST}",
            database=f"{INFLUX_DB}",
            # token=f"{INFLUX_TOKEN}",
            write_client_options=wco
            )
        print("Ok")

    def make_inference(self, request, context):
        request_time=datetime.fromtimestamp(int(request.datetime.seconds))
        print(f"Received inference request for datetime: {request_time}, ts: {request_time.timestamp()}")

        print("Querying InfluxDB:",end=" ")
        query_result = self.influx_client.query(f"""
                                                SELECT *
                                                FROM internet
                                                WHERE time <= to_timestamp('{request_time}')
                                                ORDER BY time DESC
                                                LIMIT {self.window_size}
                                                """,mode="pandas")
        print("Ok")

        print("Preprocessing input:",end=" ")
        x= self.scaler.transform(query_result[["internet"]].values[::-1]).reshape(1,self.window_size,1)
        print("Ok")

        print(f"Performing inference:")
        y_pred=self.model.predict(x).flatten().tolist()
        print("Ok")

        print(f"X:\n{x}\n Y_pred:\n{y_pred}\n")

        return inference_pb2.InferenceResult(predictions=y_pred)

def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    inference_pb2_grpc.add_InferenceServicer_to_server(
        MultiStepInferenceServicer(), server
    )
    server.add_insecure_port("[::]:50051")
    server.start()
    server.wait_for_termination()


if __name__ == "__main__":
    serve()
    