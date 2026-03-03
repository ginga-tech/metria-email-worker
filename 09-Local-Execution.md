# Local Execution

## Run RabbitMQ (Docker)

docker run -d \
  --hostname rabbit \
  --name rabbit \
  -p 5672:5672 \
  -p 15672:15672 \
  rabbitmq:3-management

## Run Worker

dotnet restore
dotnet ef database update
dotnet run --project src/Metria.EmailWorker.Worker