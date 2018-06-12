FROM microsoft/dotnet:2.1-runtime
WORKDIR /app

WORKDIR /app
COPY ./obj/Docker/publish /app
ENTRYPOINT ["dotnet", "CloudTrailSearch.dll"]
