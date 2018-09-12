# packages-toezicht-dossier-service
Microservice that packages a toezicht dossier by creating a zip file with the linked files and metadata files.

## Installation
To add the service to your stack, add the following snippet to docker-compose.yml:

```
services:
  packagetoezicht:
    image: lblod/package-toezicht-dossier-service
```

## REST API
### POST /package-toezicht-dossiers/
Trigger packaging
Returns `202 Accepted` if the process was started

Returns `503 Service Unavailable` if already running

## Development

```
services:
  packagetoezicht:
    image: semtech/mu-javascript-template
    ports:
      - 8888:80
    environment:
      NODE_ENV: "development"
    volumes:
      - /path/to/your/code:/app/
```
