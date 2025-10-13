import requests
import os
import json
import time
import logging

# ตั้งค่า Logging
logging.basicConfig(level=logging.INFO, format='%(levelname)s:%(name)s:%(message)s')
logger = logging.getLogger(__name__)

# ดึง URL จาก Environment Variables
CAMUNDA_REST_URL = os.environ.get("CAMUNDA_REST_URL", "http://operaton:8080/engine-rest")
FASTAPI_URL = os.environ.get("FASTAPI_URL", "http://fastapi-api:8000/api/v1")

# กำหนด Topic เดียวสำหรับ External HTTP Calls ทั้งหมด
GENERIC_HTTP_TOPIC = "http-request-topic"
TOPICS = [GENERIC_HTTP_TOPIC]

WORKER_ID = "generic-http-worker"
MAX_TASKS = 5
LOCK_DURATION_MS = 30000 # 30 วินาที
POLL_INTERVAL_SEC = 5 # 5 วินาที

# --- Helper Functions ---

def get_variable_value(variables, name, default=""):
    """ฟังก์ชันดึงค่าตัวแปรอย่างปลอดภัย"""
    try:
        # Camunda API ส่งตัวแปรมาในรูปแบบ {"variableName": {"value": "...", "type": "..."}}
        return variables.get(name, {}).get("value", default)
    except Exception as e:
        logger.error(f"Error extracting variable '{name}': {e}")
        return default

def complete_task(task_id):
    """ส่งสัญญาณ Complete กลับไปยัง Operaton Engine"""
    url = f"{CAMUNDA_REST_URL}/external-task/{task_id}/complete"
    payload = {"workerId": WORKER_ID, "variables": {}}
    try:
        requests.post(url, json=payload).raise_for_status()
        logger.info(f"Task {task_id} completed successfully.")
        return True
    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to complete task {task_id}. Error: {e}")
        return False

def handle_failure(task_id, error_message):
    """จัดการเมื่อ Worker ประมวลผลล้มเหลว"""
    url = f"{CAMUNDA_REST_URL}/external-task/{task_id}/failure"
    payload = {
        "workerId": WORKER_ID,
        "errorMessage": "Generic HTTP Worker failed to execute request.",
        "errorDetails": error_message,
        "retries": 0, 
        "retryTimeout": 60000 
    }
    try:
        requests.post(url, json=payload).raise_for_status()
        logger.warning(f"Task {task_id} reported failure to Engine.")
    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to report failure for task {task_id}. Error: {e}")

# --- Generic Task Handler ---

def handle_generic_http_request(task_id, variables):
    """
    จัดการ External Task โดยการสร้างและส่ง HTTP Request ไปยัง URL ที่กำหนด 
    โดยใช้ตัวแปร BPMN: requestUrl, httpMethod, requestPayload
    """
    
    # ดึงตัวแปรที่จำเป็นในการสร้าง Request
    http_method = get_variable_value(variables, "httpMethod", "POST").upper()
    request_url = get_variable_value(variables, "requestUrl")
    payload_str = get_variable_value(variables, "requestPayload", "{}")

    if not request_url:
        error_msg = "BPMN variable 'requestUrl' is missing or empty."
        logger.error(error_msg)
        handle_failure(task_id, error_msg)
        return False

    try:
        # 1. Parse Payload (Body)
        payload = json.loads(payload_str)
        
        # 2. ส่ง HTTP Request
        logger.info(f"Executing {http_method} to {request_url} with payload: {payload}")
        
        response = requests.request(
            method=http_method,
            url=request_url,
            json=payload,
            timeout=10 # กำหนด Timeout 10 วินาที
        )
        response.raise_for_status() # ตรวจสอบ HTTP Status (4xx, 5xx)

        logger.info(f"Request to {request_url} successful. Status: {response.status_code}")
        return True
        
    except json.JSONDecodeError as e:
        error_msg = f"Failed to parse 'requestPayload' JSON. Error: {e}"
        handle_failure(task_id, error_msg)
    except requests.exceptions.RequestException as e:
        error_msg = f"HTTP Request Failed ({http_method} {request_url}). Error: {e}"
        handle_failure(task_id, error_msg)
    except Exception as e:
        error_msg = f"An unexpected error occurred: {e}"
        handle_failure(task_id, error_msg)
        
    return False

def process_tasks(tasks):
    """กระจายงานไปยัง Task Handler ที่เหมาะสม"""
    for task in tasks:
        task_id = task.get("id")
        topic = task.get("topicName")
        variables = task.get("variables", {})
        
        logger.info(f"Processing Task ID: {task_id} with Topic: {topic}")

        success = False
        if topic == GENERIC_HTTP_TOPIC:
            # เรียก handler และเก็บผลลัพธ์ความสำเร็จ
            success = handle_generic_http_request(task_id, variables)
        else:
            logger.warning(f"Unknown topic: {topic}. Skipping.")
            
        # ตรวจสอบผลลัพธ์: ถ้าสำเร็จ ให้เรียก complete_task
        if success:
            complete_task(task_id)

def fetch_and_lock():
    """ดึงและล็อคงามจาก Operaton Engine"""
    url = f"{CAMUNDA_REST_URL}/external-task/fetchAndLock"
    payload = {
        "workerId": WORKER_ID,
        "maxTasks": MAX_TASKS,
        "usePriority": True,
        "topics": [{"topicName": t, "lockDuration": LOCK_DURATION_MS} for t in TOPICS]
    }
    
    try:
        response = requests.post(url, json=payload)
        response.raise_for_status()
        tasks = response.json()
        
        if tasks:
            logger.info(f"Fetched {len(tasks)} tasks.")
            process_tasks(tasks)
        else:
            logger.info("No tasks to fetch. Waiting...")

    except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching tasks from Camunda Engine: {e}")

if __name__ == "__main__":
    logger.info(f"Starting Generic HTTP Worker. Target Engine: {CAMUNDA_REST_URL}. Target API (Internal Base): {FASTAPI_URL}. Polling...")
    while True:
        fetch_and_lock()
        time.sleep(POLL_INTERVAL_SEC)
