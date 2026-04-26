FROM mcr.microsoft.com/dotnet/sdk:8.0 AS build
WORKDIR /src

COPY examples/RpcDockerExample/Consumer/Consumer.csproj examples/RpcDockerExample/Consumer/

RUN dotnet restore examples/RpcDockerExample/Consumer/Consumer.csproj

COPY examples/RpcDockerExample/Consumer/ examples/RpcDockerExample/Consumer/

RUN dotnet publish examples/RpcDockerExample/Consumer/Consumer.csproj \
    --configuration Release \
    --output /app/publish

FROM mcr.microsoft.com/dotnet/runtime:8.0 AS final
WORKDIR /app
COPY --from=build /app/publish .
ENTRYPOINT ["dotnet", "Consumer.dll"]
