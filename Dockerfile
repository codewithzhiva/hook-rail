FROM golang:1.26-alpine AS build
WORKDIR /src
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /hookrail ./cmd/server
FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /hookrail /hookrail
VOLUME ["/data"]
EXPOSE 8080
ENTRYPOINT ["/hookrail"]
