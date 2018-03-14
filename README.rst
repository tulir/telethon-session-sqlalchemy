Telethon SQLAlchemy session
===========================

A `Telethon`_ session storage implementation backed by `SQLAlchemy`_.

.. _Telethon: https://github.com/LonamiWebs/Telethon
.. _SQLAlchemy: https://www.sqlalchemy.org/

Installation
------------
`telethon-session-sqlalchemy`_ @ PyPI

.. code-block:: shell

    pip install telethon-session-sqlalchemy

.. _telethon-session-sqlalchemy: https://pypi.python.org/pypi/telethon-session-sqlalchemy

Usage
-----
This session implementation can store multiple Sessions in the same database,
but to do this, each session instance needs to have access to the same models
and database session.

To get started, you need to create an ``AlchemySessionContainer`` which will
contain that shared data. The simplest way to use ``AlchemySessionContainer``
is to simply pass it the database URL:

.. code-block:: python

    from alchemysession import AlchemySessionContainer
    container = AlchemySessionContainer('mysql://user:pass@localhost/telethon')

If you already have SQLAlchemy set up for your own project, you can also pass
the engine separately:

.. code-block:: python

    my_sqlalchemy_engine = sqlalchemy.create_engine('...')
    container = AlchemySessionContainer(engine=my_sqlalchemy_engine)

By default, the session container will manage table creation/schema updates/etc
automatically. If you want to manage everything yourself, you can pass your
SQLAlchemy Session and ``declarative_base`` instances and set ``manage_tables``
to ``False``:

.. code-block:: python

    from sqlalchemy.ext.declarative import declarative_base
    from sqlalchemy import orm
    import sqlalchemy
    ...
    session_factory = orm.sessionmaker(bind=my_sqlalchemy_engine)
    session = session_factory()
    my_base = declarative_base()
    ...
    container = AlchemySessionContainer(
        session=session, table_base=my_base, manage_tables=False
    )

You always need to provide either ``engine`` or ``session`` to the container.
If you set ``manage_tables=False`` and provide a ``session``, ``engine`` is not
needed. In any other case, ``engine`` is always required.

After you have your ``AlchemySessionContainer`` instance created, you can
create new sessions by calling ``new_session``:

.. code-block:: python

    session = container.new_session('some session id')
    client = TelegramClient(session)

where ``some session id`` is an unique identifier for the session.
