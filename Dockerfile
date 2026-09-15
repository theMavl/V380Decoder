FROM bluenviron/mediamtx:1.21.0 AS mediamtx

FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        gcc \
        pkg-config \
        libgstreamer1.0-dev \
        libgstreamer-plugins-base1.0-dev \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /src
COPY *.csproj .
RUN dotnet restore
COPY . .
RUN dotnet publish V380Decoder.csproj -c Release -o /app
RUN gcc -O2 -Wall -Wextra \
    native/v380-gst-bridge.c \
    -o /app/v380-gst-bridge \
    $(pkg-config --cflags --libs gstreamer-1.0 gstreamer-app-1.0)

FROM mcr.microsoft.com/dotnet/aspnet:10.0
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        ffmpeg \
        gstreamer1.0-plugins-base \
        gstreamer1.0-plugins-good \
        gstreamer1.0-plugins-bad \
        gstreamer1.0-rtsp \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY --from=build /app .
COPY --from=mediamtx /mediamtx /usr/local/bin/mediamtx
EXPOSE 8554/tcp 8080/tcp
ENTRYPOINT ["./V380Decoder"]
