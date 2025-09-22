#!/bin/bash

####################################################################################################
# Script: canales_udp.sh
# Supervisión y relanzamiento automático/manual de canales UDP con FFmpeg.
# Modular, altamente comentado, soporta Intel+Nvidia/QSV/CPU, mapeo de audio robusto, logging.
# Autor: chuymex
#
# Uso en canales.txt:
#    udp://fuente1:1234 | canal1 | encoder=cpu
#    udp://fuente2:1234 | canal2 | encoder=qsv,nodeint=0,scale=1920:1080
####################################################################################################

# =====================================================================
# [1] PARÁMETROS PERSONALIZABLES Y PRESETS DE ENCODERS
# =====================================================================

# --- Parámetros por canal, sobreescribibles en canales.txt ---
declare -A CUSTOM_PARAMS_DEFAULT
CUSTOM_PARAMS_DEFAULT[nodeint]="0"        # 1 = sin desentrelazado, 0 = con desentrelazado (yadif/deinterlace_qsv)
CUSTOM_PARAMS_DEFAULT[encoder]="nvenc"    # nvenc (Nvidia), qsv (Intel), cpu, cuda
CUSTOM_PARAMS_DEFAULT[map]=""             # Mapeo manual de streams en ffmpeg
CUSTOM_PARAMS_DEFAULT[audio]="auto"       # auto = detecta español, o index
CUSTOM_PARAMS_DEFAULT[bitrate]="2M"       # Bitrate de video
CUSTOM_PARAMS_DEFAULT[scale]="1280:720"   # Resolución de salida
CUSTOM_PARAMS_DEFAULT[screen]="0"         # 1 = crop tipo cinema especial

# --- Parámetros globales de control ---
MAX_FAILS=5         # Máximo de reinicios permitidos
FAIL_WINDOW=600     # Ventana de tiempo en segundos para fallas
MAX_LOG_LINES=2000  # Máximo de líneas en log
MAX_LOG_SIZE=81920  # Máximo de tamaño en bytes de log

# --- Presets de FFmpeg por encoder ---
declare -A ENCODER_PRESETS
ENCODER_PRESETS[nvenc]="-c:v h264_nvenc -b:v 2M -bufsize 4M -preset p2 -tune 3 -g 60 -c:a aac -dts_delta_threshold 1000 -ab 128k -ar 44100 -ac 1 -f flv"
ENCODER_PRESETS[qsv]="-c:v h264_qsv -b:v 2M -preset veryfast -c:a aac -ab 128k -ar 44100 -ac 1 -f flv"
ENCODER_PRESETS[cuda]="-c:v h264_nvenc -preset 2 -tune 3 -keyint_min 30 -b:v 2048k -bt 1 -maxrate 2048k -bufsize 4096k -c:a aac -ar 44100 -ac 1 -ab 192k -f flv"
ENCODER_PRESETS[cpu]="-c:v libx264 -b:v 2M -preset veryfast -c:a aac -ab 128k -ar 44100 -ac 1 -f flv"

# --- Colores para consola ---
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
NC='\033[0m'

# =====================================================================
# [2] CONFIGURACIÓN GENERAL Y RUTAS
# =====================================================================
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
LOG_DIR="$SCRIPT_DIR/logs"
CANALES_FILE="$SCRIPT_DIR/canales.txt"
RTMP_PREFIX="rtmp://fuentes.futuretv.pro:9922/tp"

# =====================================================================
# [3] LOGGING Y INICIALIZACIÓN
# =====================================================================
echo "$(date '+%Y-%m-%d %H:%M:%S') [INFO] Inicializando logs y eliminando antiguos..."
mkdir -p "$LOG_DIR"
find "$LOG_DIR" -type f -name "*.log" -delete
declare -A FAIL_HISTORY

# =====================================================================
# [4] FUNCIONES MODULARES Y UTILITARIAS
# =====================================================================

# --- Limita tamaño y líneas de logs para evitar archivos gigantes
limitar_log() {
    local log_file="$1"
    if [[ -f "$log_file" ]]; then
        local filesize lines
        filesize=$(stat --format="%s" "$log_file")
        lines=$(wc -l < "$log_file")
        if (( lines > MAX_LOG_LINES )); then
            tail -n $MAX_LOG_LINES "$log_file" > "${log_file}.tmp" && mv "${log_file}.tmp" "$log_file"
        fi
        filesize=$(stat --format="%s" "$log_file")
        if (( filesize > MAX_LOG_SIZE )); then
            tail -c $MAX_LOG_SIZE "$log_file" > "${log_file}.tmp" && mv "${log_file}.tmp" "$log_file"
        fi
    fi
}

# --- Detecta el índice de stream de audio en español (spa) en el contenedor, retorna posición relativa
detectar_audio_spa_relativo() {
    local udp_url="$1"
    local log_file="$2"
    ffprobe -v error -show_streams -select_streams a "$udp_url" 2>/dev/null \
    | awk -v logfile="$log_file" '
        BEGIN { in_audio=0; pos=0; spa_pos=""; first_pos=""; idx=""; lang=""; block="" }
        /^\[STREAM\]$/ { block=""; in_audio=1 }
        /^\[\/STREAM\]$/ {
            if (block ~ /codec_type=audio/) {
                match(block, /index=([0-9]+)/, arr); idx=arr[1]
                match(block, /TAG:language=([a-z]+)/, arr2); lang=arr2[1]
                cmd = "echo \"      [audio_pos=" pos ", index=" idx ", lang=" lang "]\" >> " logfile
                system(cmd)
                if (lang == "spa" && spa_pos == "") spa_pos=pos
                if (first_pos == "") first_pos=pos
                pos++
            }
            in_audio=0; block=""
        }
        { if (in_audio) block = block $0 "\n" }
        END {
            if (spa_pos != "") print spa_pos;
            else if (first_pos != "") print first_pos;
            else print 0;
        }
    '
}

# --- Detecta el codec de video para decidir pipeline óptima
detectar_codec_video() {
    local udp_url="$1"
    ffprobe -v error -show_streams -select_streams v "$udp_url" 2>/dev/null | awk -F= '/^codec_name=/{print $2; exit}'
}

# --- Detecta si ffmpeg soporta filtro scale_qsv (Intel QuickSync)
detectar_soporte_scale_qsv() {
    ffmpeg -hide_banner -filters 2>&1 | grep -q 'scale_qsv'
    return $?
}

# --- Parsea parámetros personalizados por canal, los exporta como variables locales
parsear_parametros_personalizados() {
    local extra_params="$1"
    declare -A params
    for key in "${!CUSTOM_PARAMS_DEFAULT[@]}"; do
        params["$key"]="${CUSTOM_PARAMS_DEFAULT[$key]}"
    done
    if [[ -n "$extra_params" ]]; then
        IFS=',' read -ra kvs <<< "$extra_params"
        for kv in "${kvs[@]}"; do
            kv="$(echo "$kv" | xargs)"
            case "$kv" in
                nodeint=*) params[nodeint]="${kv#nodeint=}" ;;
                encoder=*) params[encoder]="${kv#encoder=}" ;;
                map=*)     params[map]="${kv#map=}" ;;
                audio=*)   params[audio]="${kv#audio=}" ;;
                bitrate=*) params[bitrate]="${kv#bitrate=}" ;;
                scale=*)   params[scale]="${kv#scale=}" ;;
                screen=*)  params[screen]="${kv#screen=}" ;;
            esac
        done
    fi
    for key in "${!params[@]}"; do
        eval "$key='${params[$key]}'"
    done
}

# --- Agrega fifo_size/overrun_nonfatal si falta en URL UDP para evitar overrun de buffer en ffmpeg
ajustar_url_udp() {
    local url="$1"
    if [[ "$url" =~ ^udp:// ]]; then
        if [[ ! "$url" =~ fifo_size ]]; then
            if [[ "$url" =~ \? ]]; then
                url="${url}&fifo_size=524288&overrun_nonfatal=1"
            else
                url="${url}?fifo_size=524288&overrun_nonfatal=1"
            fi
        fi
    fi
    echo "$url"
}

# --- Construye pipeline FFmpeg ADAPTATIVA: 
#     - Si es h264 y NVENC, usa filtros GPU (yadif_cuda, scale_cuda)
#     - Si es mpeg2 u otro, usa filtros normales
construir_pipeline_ffmpeg() {
    local udp_url="$1"
    local encoder="$2"
    local nodeint="$3"
    local scale="$4"
    local screen="$5"
    local bitrate="$6"
    local video_codec; video_codec=$(detectar_codec_video "$udp_url")

    local resize; resize=$(echo "$scale" | sed 's/:/x/')
    local filtro_screen=""
    if [[ "$screen" == "1" ]]; then
        filtro_screen='crop=w=ih*16/9:h=ih,scale=1280:720'
    fi

    FF_PREINPUT=""
    FF_FILTER=""
    FF_ENCODE=""

    # --- CPU encoder (siempre filtros normales) ---
    if [[ "$encoder" == "cpu" ]]; then
        FF_PREINPUT="-y"
        FF_FILTER="-vf yadif,scale=${scale}"
        FF_ENCODE="${ENCODER_PRESETS[cpu]}"

    # --- QSV encoder (Intel) ---
    elif [[ "$encoder" == "qsv" ]]; then
        if [[ "$video_codec" == "mpeg2video" ]]; then
            FF_PREINPUT="-y -hwaccel qsv -c:v mpeg2_qsv"
        else
            FF_PREINPUT="-y -hwaccel qsv -c:v ${video_codec}_qsv"
        fi
        if [[ -n "$filtro_screen" ]]; then
            FF_FILTER="-vf $filtro_screen"
        else
            if [[ "$nodeint" == "1" ]]; then
                if detectar_soporte_scale_qsv; then
                    FF_FILTER="-vf scale_qsv=w=$(echo "$scale" | cut -d: -f1):h=$(echo "$scale" | cut -d: -f2)"
                else
                    FF_FILTER="-vf scale=${scale}"
                fi
            else
                if ffmpeg -hide_banner -filters 2>&1 | grep -q 'deinterlace_qsv'; then
                    FF_FILTER="-vf deinterlace_qsv,scale_qsv=w=$(echo "$scale" | cut -d: -f1):h=$(echo "$scale" | cut -d: -f2)"
                elif detectar_soporte_scale_qsv; then
                    FF_FILTER="-vf yadif,scale_qsv=w=$(echo "$scale" | cut -d: -f1):h=$(echo "$scale" | cut -d: -f2)"
                else
                    FF_FILTER="-vf yadif,scale=${scale}"
                fi
            fi
        fi
        FF_ENCODE="${ENCODER_PRESETS[qsv]}"

    # --- CUDA encoder (usa filtros GPU) ---
    elif [[ "$encoder" == "cuda" ]]; then
        FF_PREINPUT="-y -hwaccel cuda -hwaccel_output_format cuda"
        FF_FILTER="-vf yadif_cuda,scale_cuda=w=$(echo "$scale" | cut -d: -f1):h=$(echo "$scale" | cut -d: -f2)"
        FF_ENCODE="${ENCODER_PRESETS[cuda]}"

    # --- NVENC encoder (Nvidia) ---
    elif [[ "$encoder" == "nvenc" ]]; then
        # Si es h264, usamos decodificación por GPU y filtros GPU
        if [[ "$video_codec" == "h264" ]]; then
            FF_PREINPUT="-y -hwaccel cuvid -c:v h264_cuvid"
            if [[ -n "$filtro_screen" ]]; then
                FF_FILTER="-vf $filtro_screen"
            else
                if [[ "$nodeint" == "0" ]]; then
                    FF_FILTER="-vf yadif_cuda,scale_cuda=w=$(echo "$scale" | cut -d: -f1):h=$(echo "$scale" | cut -d: -f2)"
                else
                    FF_FILTER="-vf scale_cuda=w=$(echo "$scale" | cut -d: -f1):h=$(echo "$scale" | cut -d: -f2)"
                fi
            fi
        # Si es MPEG2 o cualquier otro, decodifica por CPU y usa filtros normales
        elif [[ "$video_codec" == "mpeg2video" ]]; then
            FF_PREINPUT="-y -c:v mpeg2video"
            if [[ -n "$filtro_screen" ]]; then
                FF_FILTER="-vf $filtro_screen"
            else
                if [[ "$nodeint" == "1" ]]; then
                    FF_FILTER="-vf scale=${scale}"
                else
                    FF_FILTER="-vf yadif,scale=${scale}"
                fi
            fi
        else
            # Para otros codecs, decodifica por CPU y usa filtros normales
            FF_PREINPUT="-y"
            if [[ -n "$filtro_screen" ]]; then
                FF_FILTER="-vf $filtro_screen"
            else
                if [[ "$nodeint" == "1" ]]; then
                    FF_FILTER="-vf scale=${scale}"
                else
                    FF_FILTER="-vf yadif,scale=${scale}"
                fi
            fi
        fi
        FF_ENCODE="${ENCODER_PRESETS[nvenc]}"

    # --- Default: decodifica por CPU y usa filtros normales ---
    else
        FF_PREINPUT="-y"
        FF_FILTER="-vf scale=${scale}"
        FF_ENCODE="${ENCODER_PRESETS[nvenc]}"
    fi
}

# --- Determina el mapeo de audio (manual o auto), retorna opción para ffmpeg
determinar_mapeo_audio() {
    local udp_url="$1"
    local log_file="$2"
    local audio="$3"
    local map="$4"

    if [[ -n "$map" ]]; then
        echo "$(date '+%Y-%m-%d %H:%M:%S') [INFO] Mapeo manual aplicado: $map" | tee -a "$log_file"
        echo "$map"
    else
        echo "$(date '+%Y-%m-%d %H:%M:%S') [INFO] Audio streams detectados (orden y index):" | tee -a "$log_file"
        local audio_idx
        if [[ "$audio" == "auto" ]]; then
            audio_idx=$(detectar_audio_spa_relativo "$udp_url" "$log_file")
        else
            audio_idx=$((audio-1))
        fi
        echo "$(date '+%Y-%m-%d %H:%M:%S') [INFO] Mapeo automático: audio_idx=$audio_idx (usando idioma spa si existe, posición real en contenedor)" | tee -a "$log_file"
        echo "-map 0:v -map 0:a:$audio_idx"
    fi
}

# --- Arma el pipeline del canal y lanza ffmpeg en segundo plano, loguea todo
lanzar_canal() {
    local udp_url="$1"
    local canal_nombre="$2"
    local extra_params="$3"
    local rtmp_url="$RTMP_PREFIX/$canal_nombre"
    local log_file="$LOG_DIR/$canal_nombre.log"

    parsear_parametros_personalizados "$extra_params"
    udp_url=$(ajustar_url_udp "$udp_url")

    echo "$(date '+%Y-%m-%d %H:%M:%S') [INFO] Lanzando canal: $canal_nombre" | tee -a "$log_file"
    echo "$(date '+%Y-%m-%d %H:%M:%S') [INFO] Parámetros personalizados: nodeint=$nodeint, encoder=$encoder, map='$map', audio=$audio, bitrate=$bitrate, scale=$scale, screen=$screen" | tee -a "$log_file"
    limitar_log "$log_file"

    construir_pipeline_ffmpeg "$udp_url" "$encoder" "$nodeint" "$scale" "$screen" "$bitrate"

    local map_opt
    map_opt="$(determinar_mapeo_audio "$udp_url" "$log_file" "$audio" "$map" | tail -n 1)"
    limitar_log "$log_file"

    local ffmpeg_cmd="ffmpeg $FF_PREINPUT -i $udp_url $FF_FILTER $FF_ENCODE $map_opt $rtmp_url"
    echo "$(date '+%Y-%m-%d %H:%M:%S') [INFO] Comando FFmpeg generado: $ffmpeg_cmd" | tee -a "$log_file"
    limitar_log "$log_file"

    local pids
    pids=$(ps -eo pid,args | grep "[f]fmpeg" | awk -v url="$rtmp_url" '
    {
        for(i=2;i<=NF;i++) {
            if ($i == url) print $1
        }
    }')
    if [[ -n "$pids" ]]; then
        for pid in $pids; do
            kill -9 "$pid"
            echo "$(date '+%Y-%m-%d %H:%M:%S') [WARN] Proceso ffmpeg (PID $pid) eliminado por duplicado." >> "$log_file"
            limitar_log "$log_file"
        done
        sleep 1
    fi

    echo "$(date '+%Y-%m-%d %H:%M:%S') [INFO] Diagnóstico previo UDP con ffprobe..." >> "$log_file"
    limitar_log "$log_file"
    timeout 8 ffprobe "$udp_url" >> "$log_file" 2>&1 || \
        echo "$(date '+%Y-%m-%d %H:%M:%S') [WARN] ffprobe no pudo acceder a fuente UDP." >> "$log_file"
    limitar_log "$log_file"

    echo "$(date '+%Y-%m-%d %H:%M:%S') [INFO] Estado RAM/disco/GPU:" >> "$log_file"
    limitar_log "$log_file"
    free -h >> "$log_file"
    limitar_log "$log_file"
    df -h >> "$log_file"
    limitar_log "$log_file"
    command -v nvidia-smi &>/dev/null && nvidia-smi >> "$log_file"
    limitar_log "$log_file"

    echo "$(date '+%Y-%m-%d %H:%M:%S') [INFO] Lanzando ffmpeg en segundo plano para $canal_nombre" >> "$log_file"
    limitar_log "$log_file"
    nohup $ffmpeg_cmd >> "$log_file" 2>&1 &
    sleep 1
}

# =====================================================================
# [5] CARGA DE CANALES DESDE ARCHIVO
# =====================================================================
leer_canales() {
    canales=()
    while IFS= read -r line || [[ -n "$line" ]]; do
        line="${line//$'\r'/}"
        [[ -z "$line" || "$line" == \#* ]] && continue
        IFS='|' read -r udp_url canal_nombre extra_params <<< "$line"
        udp_url="$(echo "$udp_url" | sed 's/^[[:space:]]*//;s/[[:space:]]*$//')"
        canal_nombre="$(echo "$canal_nombre" | sed 's/^[[:space:]]*//;s/[[:space:]]*$//')"
        extra_params="$(echo "$extra_params" | sed 's/^[[:space:]]*//;s/[[:space:]]*$//')"
        canales+=("$udp_url|$canal_nombre|$extra_params")
    done < "$CANALES_FILE"
}

# =====================================================================
# [6] LANZAMIENTO AUTOMÁTICO DE TODOS LOS CANALES
# =====================================================================
lanzar_todos_canales() {
    for entry in "${canales[@]}"; do
        IFS='|' read -r udp_url canal_nombre extra_params <<< "$entry"
        lanzar_canal "$udp_url" "$canal_nombre" "$extra_params"
    done
}

# =====================================================================
# [7] SUPERVISIÓN Y RELANZAMIENTO AUTOMÁTICO DE CANALES (PAUSA INDIVIDUAL)
# =====================================================================
supervisar_canales() {
    declare -A PAUSA_CANAL
    while true; do
        now=$(date +%s)
        for entry in "${canales[@]}"; do
            IFS='|' read -r udp_url canal_nombre extra_params <<< "$entry"
            rtmp_url="$RTMP_PREFIX/$canal_nombre"
            log_file="$LOG_DIR/$canal_nombre.log"

            if [[ -n "${PAUSA_CANAL["$canal_nombre"]}" ]] && (( now < PAUSA_CANAL["$canal_nombre"] )); then
                echo "$(date '+%Y-%m-%d %H:%M:%S') [INFO] Supervisor: el canal $canal_nombre está en pausa hasta $(date -d @${PAUSA_CANAL["$canal_nombre"]})" >> "$log_file"
                limitar_log "$log_file"
                continue
            fi

            local pids
            pids=$(ps -eo pid,args | grep "[f]fmpeg" | awk -v url="$rtmp_url" '
            {
                for(i=2;i<=NF;i++) {
                    if ($i == url) print $1
                }
            }')
            if [[ -z "$pids" ]]; then
                FAIL_HISTORY["$canal_nombre"]+="$now "
                fails=0
                for ts in ${FAIL_HISTORY["$canal_nombre"]}; do
                    (( now - ts <= FAIL_WINDOW )) && ((fails++))
                done
                if (( fails > MAX_FAILS )); then
                    echo "$(date '+%Y-%m-%d %H:%M:%S') [ERROR] Canal $canal_nombre cayó $fails veces en los últimos $FAIL_WINDOW segundos. Pausando relanzamiento por 10 minutos." >> "$log_file"
                    limitar_log "$log_file"
                    PAUSA_CANAL["$canal_nombre"]=$((now+600))
                    continue
                fi

                echo "$(date '+%Y-%m-%d %H:%M:%S') [ERROR] Canal $canal_nombre caído. Relanzando..." >> "$log_file"
                limitar_log "$log_file"
                lanzar_canal "$udp_url" "$canal_nombre" "$extra_params"
                echo "$(date '+%Y-%m-%d %H:%M:%S') [INFO] Canal $canal_nombre relanzado por supervisor." >> "$log_file"
                limitar_log "$log_file"
            fi
        done
        sleep 60
    done
}

# =====================================================================
# [8] RELANZAMIENTO MANUAL DE CANAL
# =====================================================================
relanzar_canal_por_nombre() {
    local nombre="$1"
    leer_canales
    local encontrado=0
    for entry in "${canales[@]}"; do
        IFS='|' read -r udp_url canal_nombre extra_params <<< "$entry"
        if [[ "$canal_nombre" == "$nombre" ]]; then
            encontrado=1
            echo -e "${CYAN}Relanzando canal: ${YELLOW}$canal_nombre${NC}"
            local rtmp_url="$RTMP_PREFIX/$canal_nombre"
            local log_file="$LOG_DIR/$canal_nombre.log"
            local pids
            pids=$(ps -eo pid,args | grep "[f]fmpeg" | awk -v url="$rtmp_url" '
            {
                for(i=2;i<=NF;i++) {
                    if ($i == url) print $1
                }
            }')
            if [[ -n "$pids" ]]; then
                for pid in $pids; do
                    kill -9 "$pid"
                    echo -e "${RED}Proceso ffmpeg (PID $pid) para $canal_nombre eliminado.${NC}"
                    echo "$(date '+%Y-%m-%d %H:%M:%S') [WARN] Proceso ffmpeg (PID $pid) eliminado manualmente." >> "$log_file"
                    limitar_log "$log_file"
                done
                sleep 1
            fi
            lanzar_canal "$udp_url" "$canal_nombre" "$extra_params"
            echo -e "${GREEN}Canal ${YELLOW}$canal_nombre${GREEN} relanzado manualmente.${NC}"
            echo "$(date '+%Y-%m-%d %H:%M:%S') [INFO] Canal $canal_nombre relanzado manualmente." >> "$log_file"
            limitar_log "$log_file"
            return 0
        fi
    done
    if [[ $encontrado -eq 0 ]]; then
        echo -e "${RED}Canal '${nombre}' no encontrado en canales.txt${NC}"
        return 1
    fi
}

# =====================================================================
# [9] ENTRADA PRINCIPAL DEL SCRIPT
# =====================================================================
if [[ "$1" == "relanzar" && -n "$2" ]]; then
    relanzar_canal_por_nombre "$2"
    exit $?
fi

if [[ ! -f "$CANALES_FILE" ]]; then
    echo -e "${RED}ERROR: No se encontró el archivo canales.txt en $SCRIPT_DIR${NC}"
    exit 1
fi

leer_canales
lanzar_todos_canales
supervisar_canales

exit 0
