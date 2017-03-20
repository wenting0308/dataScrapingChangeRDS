from setuptools import setup

setup(name="scrap_realtime_data",
      version="0.1",
      description="Scrape real time station data from API",
      url="",
      author="Wen-ting, Chang",
      author_email="wen-ting.chang@ucdconnect.ie",
      licence="GPL3",
      packages=['scrapData'],
      entry_points={
        'console_scripts':['scrap_realtime_data=scrapData.main:main']
        },
      )