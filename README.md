# canales_udp.sh

## Descripción

`canales_udp.sh` es un script Bash avanzado para la administración, supervisión y relanzamiento automático/manual de flujos de canales UDP utilizando FFmpeg. Está orientado a entornos de streaming profesional (TV/IPTV/OTT), soportando transcodificación acelerada por hardware (Intel QSV, Nvidia NVENC/CUDA), crop tipo cinema, mapeo de audio robusto, logging avanzado y lógica de supervisión individual por canal.

El script permite:
- Lanzar y monitorear múltiples canales desde un archivo `canales.txt`
- Relanzamiento automático si un canal cae, con tolerancia configurable a fallos
- Relanzamiento manual de canales específicos
- Logging y diagnóstico previo por canal (RAM/disco/GPU/ffprobe)
- Presets y parámetros personalizables por canal (resolución, encoder, audio, crop, etc.)

## Requisitos

- Bash 4+
- FFmpeg (con soporte para QSV/NVENC/CUDA según hardware)
- ffprobe
- nvidia-smi (opcional, para tarjetas Nvidia)
- netcat (opcional para diagnóstico)
- Permisos de ejecución de scripts

## Formato de archivo canales.txt

Cada línea tiene la forma:
```
udp://fuente:puerto | nombredelcanal | encoder=...,nodeint=...,scale=...,audio=...,map=...,screen=...,force_mpeg2_qsv=...
```
Ejemplo:
```
udp://239.0.0.1:1234 | canal1 | encoder=cpu
udp://239.0.0.2:1234 | canal2 | encoder=qsv,nodeint=0,scale=1920:1080
udp://239.0.0.3:1234 | canal3 | encoder=nvenc,screen=1,audio=2
```
**Campos soportados:**
- `encoder`: `cpu` (x264), `qsv` (Intel), `nvenc`/`cuda` (Nvidia)
- `nodeint`: 1 (sin desentrelazado), 0 (con desentrelazado)
- `scale`: resolución de salida (ej: 1280:720)
- `audio`: `auto` (detecta español si existe), o índice (ej: 2)
- `map`: mapeo manual de streams FFmpeg
- `screen`: 1 activa crop tipo cinema
- `force_mpeg2_qsv`: 1 fuerza decoder QSV para mpeg2

## Uso

1. Edita el archivo `canales.txt` con los canales deseados.
2. Lanza el script:
   ```bash
   ./canales_udp.sh
   ```
   Esto lanzará todos los canales y activará el supervisor.

3. Para relanzar manualmente un canal:
   ```bash
   ./canales_udp.sh relanzar nombredelcanal
   ```

## Parámetros Globales Editables

Dentro del script puedes ajustar:
- `MAX_FAILS`: número de caídas permitidas antes de pausar el canal
- `FAIL_WINDOW`: ventana de tiempo para conteo de caídas (segundos)
- `MAX_LOG_LINES` y `MAX_LOG_SIZE`: límites de logs

## Ejemplo de flujo supervisado

- Si un canal cae, el script intentará relanzarlo.
- Si cae más de `MAX_FAILS` veces en `FAIL_WINDOW`, se pausa 10 minutos solo ese canal.
- El resto de canales continúa funcionando y supervisándose normalmente.

## Salidas

- Logs individuales por canal en la carpeta `logs/`.
- Estado del supervisor y acciones en los logs.
- Comando ffmpeg generado y diagnóstico de entrada/salida por canal.

## Contribuciones

Pull requests y mejoras son bienvenidos. Por favor abre un Issue si encuentras algún bug o deseas sugerir una mejora.

---
