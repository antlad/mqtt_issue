import os
import subprocess
import sys
import random
import signal

from time import sleep

mqtt_server = os.getenv("MQTT_SERVER_EXECUTABLE", "./mosquitto")
mqtt_sub = os.getenv("MQTT_SUB_EXECUTABLE", "mqtt_srv/mosquitto_sub")
mqtt_issue = os.getenv("MQTT_ISSUE_EXECUTABLE", "./mqtt_issue")

mqtt_topic = os.getenv("MQTT_TOPIC", "topic1")
SERVICE_URL = os.getenv("SERVICE_URL", "http://127.0.0.1:8083")
MQTT_SERVER_ADRESS = os.getenv("MQTT_SERVER_ADRESS", "127.0.0.1")
MQTT_SERVER_PORT = int(os.getenv("MQTT_SERVER_PORT", 8883))
mqtt_server_command = [mqtt_server,  "-c", "./mqtt.conf"]
mqtt_sub_command = """{} -h 127.0.0.1 -t {} --cafile mqtt_srv/m2mqtt_ca.crt -p 8883 --insecure -W 1000 | wc -l & """\
    .format(mqtt_sub, mqtt_topic)


def create_and_run(command, cwd=None):
    print("starting: {}".format(" ".join(command)))
    p = subprocess.Popen(command, stdout=sys.stdout, stderr=sys.stderr, cwd=cwd)
    return p


def random_sleep():
    sleep(random.randrange(1, 10))


def killall():
    os.system("killall -2 {}".format(os.path.basename(mqtt_server)))
    os.system("killall -2 {}".format(os.path.basename(mqtt_sub)))
    os.system("killall -2 {}".format(os.path.basename(mqtt_issue)))


def signal_handler(sig, frame):
    if sig == signal.SIGINT:
        killall()
        exit(0)


def main():
    killall()

    mqtt_process = create_and_run(mqtt_server_command, cwd=os.path.join(os.getcwd(), "mqtt_srv"))
    print("mqtt started")

    sleep(0.1)
    os.system(mqtt_sub_command)
    sleep(0.1)

    mqtt_issue_process = create_and_run(mqtt_issue)

    for i in range(100000):
        print("=================new cycle================================")
        random_sleep()
        os.system("killall mosquitto_sub")
        mqtt_process.kill()
        mqtt_process.wait()
        random_sleep()
        mqtt_process = create_and_run(mqtt_server_command, cwd=os.path.join(os.getcwd(), "mqtt_srv"))
        print("mqtt restarted")
        sleep(0.1)
        os.system(mqtt_sub_command)

    print("on finish")
    killall()
    print("all done")


if __name__ == '__main__':
    main()

