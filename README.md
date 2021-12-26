# CRM data conversion service

Microservice containing scripts to convert data from an MS SQL to triples.

The service must be executed ad-hoc and generates Turtle files which can be loaded as migrations in the application.

## Getting started
### Adding the service to your stack
Add the following snippet to your `docker-compose.yml` to include the conversion service in your project.

```yml
services:
  data-conversion:
    image: rollvolet/crm-data-conversion-service
    environment:
      SQL_PASSWORD: "secretPassword"
    volumes:
      - ./data/conversion:/data
```
