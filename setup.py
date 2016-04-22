from setuptools import setup, find_packages


setup(
    name='flatline',
    version='0.1.0',
    description=(
        'A script to watch Consul health checks and update AWS ASG health ' +
        'checks accordingly'
    ),
    long_description=open('README.rst').read(),
    author='Theron Luhn',
    author_email='theron@luhn.com',
    url='https://github.com/luhn/flatline',
    install_requires=[
        'requests>=2,<3',
    ],
    packages=['flatline'],
    entry_points={
        'console_scripts': ['flatline=flatline:main'],
    },
    classifiers=[
        'Programming Language :: Python',
        'Programming Language :: Python :: 3.5',
        'Development Status :: 2 - Pre-Alpha',
        'Environment :: Console',
        'License :: OSI Approved :: MIT License',
        'Intended Audience :: Developers',
        'Intended Audience :: System Administrators',
    ],
)
