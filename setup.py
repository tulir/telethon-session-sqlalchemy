import setuptools

setuptools.setup(
    name="telethon-session-sqlalchemy",
    version="0.2.15",
    url="https://github.com/tulir/telethon-session-sqlalchemy",

    author="Tulir Asokan",
    author_email="tulir@maunium.net",

    description="SQLAlchemy backend for Telethon session storage",
    long_description=open("README.rst").read(),

    packages=setuptools.find_packages(),

    install_requires=[
        "SQLAlchemy>=1.2,<2",
    ],

    classifiers=[
        "Development Status :: 4 - Beta",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.4",
        "Programming Language :: Python :: 3.5",
        "Programming Language :: Python :: 3.6",
    ],
    python_requires="~=3.4",
)

