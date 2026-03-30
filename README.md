# StockiAPP - Deploy en Hetzner

Esta guía cubre el despliegue en un VPS de Hetzner con Linux (Ubuntu/Debian), usando:
- Binario Go (linux/amd64)
- systemd como servicio
- Caddy como reverse proxy con HTTPS (Let's Encrypt)
- Postgres como base de datos obligatoria

## 1) Requisitos en el servidor

Instala dependencias básicas:

```bash
sudo apt update
sudo apt install -y caddy postgresql-client
```

> Si compilas en el servidor, también instala Go (`golang-go`) o usa el instalador oficial de Go.

## 2) Abrir puertos en Hetzner

En el firewall de Hetzner (y/o `ufw` si lo usas), permite:
- 22/tcp (SSH)
- 80/tcp (HTTP) y 443/tcp (HTTPS) para Caddy

Ejemplo con UFW:

```bash
sudo ufw allow 22/tcp
sudo ufw allow 80/tcp
sudo ufw allow 443/tcp
sudo ufw enable
```

## 3) Crear usuario y estructura de carpetas

```bash
sudo useradd --system --create-home --home-dir /srv/granempresa --shell /usr/sbin/nologin granempresa
sudo install -d -o granempresa -g granempresa /srv/granempresa/app /srv/granempresa/data
```

## 4) Construir el binario (linux/amd64)

### Opción A: compilar en el servidor

```bash
cd /srv/granempresa
sudo -u granempresa git clone <TU_REPO> src
cd /srv/granempresa/src
GOOS=linux GOARCH=amd64 go build -o /srv/granempresa/app/granempresa .
```

Copia las plantillas:

```bash
sudo -u granempresa rsync -a /srv/granempresa/src/templates /srv/granempresa/app/
```

### Opción B: compilar localmente y subir

```bash
GOOS=linux GOARCH=amd64 go build -o granempresa .
rsync -av granempresa templates/ granempresa@<IP>:/srv/granempresa/app/
```

## 5) Configurar systemd

Copia los unit files:

```bash
sudo cp deploy/systemd/granempresa.service /etc/systemd/system/granempresa.service
```

Recarga systemd y habilita servicios:

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now granempresa.service
```

Verifica estado:

```bash
sudo systemctl status granempresa.service
```

## 6) Configurar Caddy (HTTPS automático)

Edita `/etc/caddy/Caddyfile` y usa el contenido de `deploy/Caddyfile`:

```bash
sudo cp deploy/Caddyfile /etc/caddy/Caddyfile
sudo sed -i 's/example.com/TU_DOMINIO/g' /etc/caddy/Caddyfile
sudo systemctl reload caddy
```

> Asegúrate de que el DNS de `TU_DOMINIO` apunte al VPS antes de reiniciar Caddy.

## 7) Base de datos

- La app requiere Postgres.
- Configura `DATABASE_URL` o `DB_DSN` en el servicio.
- `DB_ENGINE` es opcional; si se define, debe ser `postgres`.
- No existe `DB_PATH`, `data.db` ni runtime alterno a Postgres.
- Si falta configuración válida de Postgres, la app falla al arrancar.
- No hay fallback silencioso a SQLite.
- La estrategia de backups debe hacerse con herramientas de Postgres (`pg_dump`, snapshots o respaldo administrado), no con scripts SQLite heredados.

### Nota de migración beta

- La etapa beta de compatibilidad dual terminó.
- El binario actual no abre SQLite ni ejecuta migraciones desde `data.db`.
- Si tienes datos legacy de SQLite, migra o importa esos datos a Postgres antes de arrancar esta versión.
- Las reparaciones legacy que siguen existiendo en el bootstrap actual son solo para normalizar datos ya cargados en Postgres; no son una vía de migración desde SQLite.

## 7.1) Usuario administrador inicial

Al iniciar la aplicación se crea un usuario administrador si no existe, leyendo las variables de entorno:

```bash
ADMIN_USER=admin
ADMIN_PASS=SuperSecreto123
```

Ejemplo en systemd (editar `/etc/systemd/system/granempresa.service`):

```ini
Environment=ADMIN_USER=admin
Environment=ADMIN_PASS=SuperSecreto123
```

> Si `ADMIN_USER` o `ADMIN_PASS` no están definidos, la app omite la creación automática.
> Si ya existe un usuario con ese `username`, no se vuelve a crear.

## 7.2) Desarrollo local con Postgres

Para desarrollo local, usa Postgres como motor principal:

```bash
cp .env.example .env.local
export DATABASE_URL=postgres://stockiapp:stockiapp@127.0.0.1:5432/stockiapp_dev?sslmode=disable
export PORT=8092
go run .
```

Atajo recomendado:

```bash
./stockiapp
```

Para detener el Postgres local:

```bash
./scripts/stop-postgres-local.sh
```

Notas:

- La app no carga `.env` automáticamente; `.env.example` es solo plantilla/documentación.
- Puedes usar `DB_DSN` en lugar de `DATABASE_URL`; el runtime trata ambas como equivalentes.
- No uses `DB_PATH` ni variables heredadas de SQLite.
- No subas `.env.local`, dumps ni datos locales al repo.
- Mantén las credenciales reales fuera de archivos versionados.

## 8) Ajustes útiles

- Cambiar el puerto interno: edita `Environment=PORT=8080` en `/etc/systemd/system/granempresa.service`.
- Cambiar la conexión a Postgres: edita `Environment=DATABASE_URL=postgres://...` o `Environment=DB_DSN=postgres://...`.
- Si defines `DB_ENGINE`, usa `Environment=DB_ENGINE=postgres`.
- Ver logs: `journalctl -u granempresa.service -f`
