FROM mcr.microsoft.com/dotnet/sdk:10.0-preview AS build
WORKDIR /src

COPY . .
RUN dotnet restore metria-email-worker.slnx
RUN dotnet publish src/Metria.EmailWorker.Processor/Metria.EmailWorker.Processor.csproj -c Release -o /app/publish /p:UseAppHost=false

FROM mcr.microsoft.com/dotnet/runtime:10.0-preview AS runtime
WORKDIR /app

COPY --from=build /app/publish .

ENTRYPOINT ["dotnet", "Metria.EmailWorker.Processor.dll"]

