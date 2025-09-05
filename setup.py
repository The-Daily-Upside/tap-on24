from setuptools import setup, find_packages

setup(
    name="tap-on24",
    version="0.1.0",
    description="A Singer tap for ON24 Webinar Platform.",
    author="The Daily Upside",
    author_email="dev@thedailyupside.com",
    packages=find_packages(),
    install_requires=[
        "singer-sdk>=0.13.0",
        "requests>=2.25.1",
    ],
    entry_points={
        "console_scripts": [
            "tap-on24=tap_on24.tap:TapON24.cli",
        ],
    },
)