from setuptools import setup
import pyhlld

# Get the long description by reading the README
try:
    readme_content = open("README.md").read()
except:
    readme_content = ""

# Create the actual setup method
setup(name='pyhlld',
      version=pyhlld.__version__,
      description='Client library to interface with hlld servers',
      long_description=readme_content,
      author='Armon Dadgar',
      author_email='armon.dadgar@gmail.com',
      maintainer='Armon Dadgar',
      maintainer_email='armon.dadgar@gmail.com',
      url="https://github.com/armon/pyhlld",
      license="MIT License",
      keywords=["hyperloglog", "hll", "hlld", "client"],
      py_modules=['pyhlld'],
      classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Operating System :: POSIX",
        "Programming Language :: Python",
        "Topic :: Database",
        "Topic :: Internet",
        "Topic :: Software Development :: Libraries",
    ]
      )
