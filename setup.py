import setuptools

setuptools.setup(
    name="KafkaHandler",
    version="0.0.1",
    author="Thales Ramos",
    author_email="thales.ramos@datameaning.com",
    description="A kafka handler class",
    download_url="https://tramos8@jira.hilton.com/stash/scm/dna/automated-data-quality-monitoring.git",
    packages=setuptools.find_packages(),
    install_requires=[            # I get to this in a second
          'confluent_kafka'
      ],
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
)