from fastapi import WebSocket, WebSocketDisconnect
from redis import Redis
import asyncio
import json
from core.config import settings
from utils.logger import logger

async def progress_websocket(websocket: WebSocket, job_id: str):
    """
    WebSocket endpoint que escucha Redis pub/sub para progress updates.
    Frontend se conecta a: ws://localhost:8000/ws/progress/{job_id}

    Args:
        websocket: Conexión WebSocket
        job_id: ID del job a monitorear
    """
    await websocket.accept()
    logger.info(f"[WEBSOCKET] Client connected for job {job_id}")

    redis = Redis.from_url(settings.redis_url, decode_responses=True)
    pubsub = redis.pubsub()
    pubsub.subscribe(f"progress:{job_id}")

    try:
        # Enviar estado inicial
        initial_state = redis.get(f"job:{job_id}")
        if initial_state:
            await websocket.send_json(json.loads(initial_state))
            logger.debug(f"[WEBSOCKET] Sent initial state for job {job_id}")
        else:
            await websocket.send_json({
                "error": "Job not found",
                "job_id": job_id
            })
            return

        # Escuchar updates de Redis pub/sub
        while True:
            message = pubsub.get_message(timeout=1.0)

            if message and message['type'] == 'message':
                data = json.loads(message['data'])
                await websocket.send_json(data)
                logger.debug(f"[WEBSOCKET] Sent update for job {job_id}: {data['status']}")

                # Cerrar si terminó (completado o fallido)
                if data.get('status').upper() in ['COMPLETED', 'FAILED', 'CANCELLED']:
                    logger.info(f"[WEBSOCKET] Job {job_id} finished with status {data['status']}")
                    break

            await asyncio.sleep(0.1)

    except WebSocketDisconnect:
        logger.info(f"[WEBSOCKET] Client disconnected for job {job_id}")

    except Exception as e:
        logger.error(f"[WEBSOCKET] Error for job {job_id}: {str(e)}")

    finally:
        pubsub.unsubscribe(f"progress:{job_id}")
        await websocket.close()
        logger.info(f"[WEBSOCKET] Connection closed for job {job_id}")
