from fastapi import APIRouter, HTTPException
import os
import json

router = APIRouter()

CONFIG_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "pipeline", "store_layouts")

@router.get("/stores/{store_id}/config")
async def get_store_config(store_id: str):
    """
    Returns the JSON configuration for all cameras in the store.
    """
    if not os.path.exists(CONFIG_DIR):
        return {"error": "Config directory not found"}
        
    config_data = {}
    for filename in os.listdir(CONFIG_DIR):
        if filename.endswith(".json"):
            filepath = os.path.join(CONFIG_DIR, filename)
            try:
                with open(filepath, "r") as f:
                    data = json.load(f)
                    camera_id = filename.replace(".json", "")
                    config_data[camera_id] = data
            except Exception as e:
                print(f"Error loading {filename}: {e}")
                
    return {"store_id": store_id, "layouts": config_data}

@router.put("/stores/{store_id}/config")
async def update_store_config(store_id: str, payload: dict):
    """
    Updates the JSON configuration for a specific camera.
    Expects a payload like {"camera_id": "CAM_FLOOR_01", "data": {...}}
    """
    camera_id = payload.get("camera_id")
    data = payload.get("data")
    
    if not camera_id or not data:
        raise HTTPException(status_code=400, detail="Missing camera_id or data")
        
    filepath = os.path.join(CONFIG_DIR, f"{camera_id}.json")
    if not os.path.exists(filepath):
        raise HTTPException(status_code=404, detail="Camera config not found")
        
    try:
        with open(filepath, "w") as f:
            json.dump(data, f, indent=2)
        return {"status": "success", "message": f"Updated {camera_id}.json"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
