from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
import time

# Launch browser and open api countries
drv = webdriver.Chrome()
drv.maximize_window()
drv.get("https://www.apicountries.com/")

box = drv.find_element(By.LINK_TEXT, "Getting Started")
box.click()

# Wait and close browser
time.sleep(5)
drv.quit()
