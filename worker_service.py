import requests
import time
import json
import logging
import os

# ดึง URL จาก Environment Variable
# CAMUNDA_REST_URL ควรชี้ไปที่ IP/Hostname ของเครื่อง Operaton (Machine A)
CAMUNDA_URL = os.environ.get("CAMUNDA_REST_URL", "http://operaton:8080/engine-rest")

# FASTAPI_BASE_URL ใช้ชื่อ Service ภายใน Docker Network (เพราะ Worker และ API รันบนเครื่องเดียวกัน)
FASTAPI_URL = os.environ.get("FASTAPI_BASE_URL", "http://fastapi-api:8000/api/v1") 

# กำหนด Worker ID ที่ไม่ซ้ำกัน
WORKER_ID = "fastapi_integration_worker"
TOPICS = ["save-db-topic", "send-email-topic"]

logging.basicConfig(level=logging.INFO)

# --- ฟังก์ชันช่วยเหลือเพื่อดึงค่าตัวแปรอย่างปลอดภัย ---
def get_variable_value(variables, name):
    """
    ดึงค่า 'value' จากตัวแปร Camunda อย่างปลอดภัย
    คืนค่าเป็นสตริงเปล่าถ้าตัวแปรไม่มีอยู่หรือมีค่าเป็น None
    """
    var_obj = variables.get(name)
    if var_obj and 'value' in var_obj:
        # Camunda ส่งค่าตัวเลขหรือ boolean มาเป็นสตริง ดังนั้นจึงแปลงเป็นสตริงเสมอ
        return str(var_obj['value'])
    return ""

# --- ฟังก์ชันการเรียก FastAPI ---

def call_fastapi_save_db(variables):
    """เรียก /api/v1/save-db ด้วยตัวแปรจาก BPMN"""
    # ใช้ get_variable_value เพื่อดึงค่าอย่างปลอดภัย
    payload = {
        "employeeName": get_variable_value(variables, "employeeName"),
        "leaveDate": get_variable_value(variables, "leaveDate"),
        "reason": get_variable_value(variables, "reason"),
        "approved": get_variable_value(variables, "approved"),
    }
    logging.info(f"Calling FastAPI Save DB with payload: {payload}")
    
    # ตรวจสอบการเชื่อมต่อกับ FastAPI API
    response = requests.post(f"{FASTAPI_URL}/save-db", json=payload)
    response.raise_for_status() 
    
    return {"status": "db saved successfully"}

def call_fastapi_send_email(variables):
    """เรียก /api/v1/send-email ด้วยตัวแปรจาก BPMN"""
    # ใช้ get_variable_value เพื่อดึงค่าอย่างปลอดภัย
    payload = {
        "employeeName": get_variable_value(variables, "employeeName"),
        "approved": get_variable_value(variables, "approved"),
    }
    logging.info(f"Calling FastAPI Send Email with payload: {payload}")
    
    # ตรวจสอบการเชื่อมต่อกับ FastAPI API
    response = requests.post(f"{FASTAPI_URL}/send-email", json=payload)
    response.raise_for_status() 

    return {"status": "email sent successfully"}

# --- Worker Logic หลัก ---

def fetch_and_lock():
    """ดึงงานจาก Engine และ Lock งานนั้น"""
    body = {
        "workerId": WORKER_ID,
        "maxTasks": 5,
        "usePriority": True,
        "asyncResponseTimeout": 30000,
        "topics": [
            {"topicName": "save-db-topic", "lockDuration": 60000},
            {"topicName": "send-email-topic", "lockDuration": 60000},
        ]
    }
    
    try:
        response = requests.post(
            f"{CAMUNDA_URL}/external-task/fetchAndLock",
            headers={"Content-Type": "application/json"},
            data=json.dumps(body)
        )
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        # รายงานข้อผิดพลาดในการเชื่อมต่อ Engine
        logging.error(f"Error fetching tasks from {CAMUNDA_URL}: {e}") 
        return []

def complete_task(task_id):
    """ส่งสัญญาณให้ Engine ทราบว่างานเสร็จสิ้นแล้ว"""
    body = {"workerId": WORKER_ID}
    try:
        requests.post(
            f"{CAMUNDA_URL}/external-task/{task_id}/complete",
            headers={"Content-Type": "application/json"},
            data=json.dumps(body)
        ).raise_for_status()
        logging.info(f"Task {task_id} completed successfully.")
    except requests.exceptions.RequestException as e:
        logging.error(f"Error completing task {task_id}: {e}")

def handle_tasks(tasks):
    """จัดการ Task ที่ถูกดึงมา"""
    for task in tasks:
        task_id = task['id']
        topic = task['topicName']
        variables = task['variables']
        
        logging.info(f"Processing Task ID: {task_id} with Topic: {topic}")
        
        try:
            if topic == "save-db-topic":
                call_fastapi_save_db(variables)
            elif topic == "send-email-topic":
                call_fastapi_send_email(variables)
            
            # เมื่อทำงานสำเร็จ Worker จะส่งสัญญาณ Complete กลับไปที่ Engine 
            complete_task(task_id)

        except Exception as e:
            # หากเกิดข้อผิดพลาดในการเรียก API (เช่น 500 Internal Error) Task จะไม่ Complete
            # และจะถูกปล่อยให้ Worker อื่นดึงไปทำใหม่เมื่อ Lock หมดอายุ
            logging.error(f"Failed to process task {task_id} (Topic: {topic}): {e}")

def run_worker():
    logging.info(f"Starting External Task Worker. Target Engine: {CAMUNDA_URL}. Target API: {FASTAPI_URL}. Polling...")
    while True:
        tasks = fetch_and_lock()
        if tasks:
            logging.info(f"Fetched {len(tasks)} tasks.")
            handle_tasks(tasks)
        else:
            logging.info("No tasks to fetch. Waiting...")
        
        time.sleep(5)

if __name__ == "__main__":
    run_worker()
