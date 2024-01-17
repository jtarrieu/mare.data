from time import sleep
import logging

logging.info('Dev mode enabled, keeping the container alive.')
elasped_time = 0
while True:
    sleep(10)
    elasped_time += 1
    logging.info(f'Container alive [{elasped_time}s]')
    