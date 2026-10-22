# syntax=docker/dockerfile:1
# One parameterised image recipe for every .NET app in the repo (PROJECT = project folder name).
FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
ARG PROJECT
WORKDIR /src

# Restore layer: only project files, so code edits don't invalidate the package cache.
COPY Directory.Build.props ./
COPY src/Fleet.Contracts/Fleet.Contracts.csproj src/Fleet.Contracts/
COPY src/Fleet.Scenario/Fleet.Scenario.csproj src/Fleet.Scenario/
COPY src/Fleet.CacheWorker/Fleet.CacheWorker.csproj src/Fleet.CacheWorker/
COPY src/Fleet.MockWriters/Fleet.MockWriters.csproj src/Fleet.MockWriters/
COPY src/Fleet.E2E/Fleet.E2E.csproj src/Fleet.E2E/
RUN dotnet restore src/${PROJECT}/${PROJECT}.csproj

COPY src/ src/
RUN dotnet publish src/${PROJECT}/${PROJECT}.csproj -c Release --no-restore -o /app \
 && cp /app/${PROJECT} /app/entrypoint

# Chiseled: no shell, no package manager, non-root by default.
FROM mcr.microsoft.com/dotnet/runtime:10.0-noble-chiseled
WORKDIR /app
COPY --from=build /app .
ENTRYPOINT ["/app/entrypoint"]
