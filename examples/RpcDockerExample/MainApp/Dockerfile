FROM mcr.microsoft.com/dotnet/sdk:8.0 AS build
WORKDIR /src

COPY src/NetTaskPipeline/NetTaskPipeline.csproj src/NetTaskPipeline/
COPY examples/RpcDockerExample/MainApp/MainApp.csproj examples/RpcDockerExample/MainApp/

RUN dotnet restore examples/RpcDockerExample/MainApp/MainApp.csproj

COPY src/NetTaskPipeline/ src/NetTaskPipeline/
COPY examples/RpcDockerExample/MainApp/ examples/RpcDockerExample/MainApp/

RUN dotnet publish examples/RpcDockerExample/MainApp/MainApp.csproj \
    --configuration Release \
    --output /app/publish \
    --no-restore

FROM mcr.microsoft.com/dotnet/runtime:8.0 AS final
WORKDIR /app
COPY --from=build /app/publish .
ENTRYPOINT ["dotnet", "MainApp.dll"]
