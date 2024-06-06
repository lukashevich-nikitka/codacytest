import time

import requests
import whatismyip
from retry import retry

from config.settings import MANAGEMENT_SCRIPT_STATUS_API, MANAGEMENT_SCRIPT_TOKEN
from config.settings import MODEL_NAME, ACTION_TYPE
from config.logger import configure_logger

ip = None
log_flow = configure_logger("Status_sender")


@retry(tries=2)
def get_ip_address() -> str:
    try:
        global ip
        if ip:
            return ip

        ip = whatismyip.whatismyip()

        if ip:
            log_flow.info(f"Response_0: {ip}")
            return ip

        urls: list[str] = ["https://www.trackip.net/ip",
                'https://api.ipify.org', 'https://checkip.amazonaws.com']
        
        headers: dict[str, str] = {
            "Content-Type": "application/json",
        }

        for i, url in enumerate(urls, 1):
            response = requests.get(url, headers=headers)
            if response.status_code == 200:
                ip = response.content.decode('utf8').strip()
                log_flow.info(f"Response_{i}: {ip}")
                return ip
            else:
                log_flow.error(
                    f"Error during request to get ip API:/nError: {response.status_code} - {response.text}")
    except requests.exceptions.RequestException as e:
        log_flow.error(
            f"Request failed:/nError: {repr(e)}")
        time.sleep(5)
        raise e


def get_status_data():
    status_data: dict[str, str] = {
        "ip": get_ip_address(),
        "api_name": MODEL_NAME,
        "action_type": ACTION_TYPE
    }
    return status_data


@retry(tries=2)
def send_status() -> None:
    try:
        headers: dict[str, str] = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {MANAGEMENT_SCRIPT_TOKEN}",
        }
        data: dict[str, str] = get_status_data()
        log_flow.info(data)
        response = requests.post(
            MANAGEMENT_SCRIPT_STATUS_API, json=data, headers=headers)

        if response.status_code == 200:
            data = response.json()
            log_flow.info(
                f"Successfully sent status to management script. Response: {data}")
        else:
            log_flow.error(
                f"Error during sending request to management script:/nError: {response.status_code} - {response.text}")
    except requests.exceptions.RequestException as e:
        log_flow.error(
            f"Request failed:/nError: {repr(e)}")
