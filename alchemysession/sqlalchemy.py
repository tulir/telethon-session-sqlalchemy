from typing import Optional, Tuple, Any, Union
import datetime

from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy import Column, String, Integer, BigInteger, LargeBinary, orm, and_, select
import sqlalchemy as sql

from telethon.sessions.memory import MemorySession, _SentFileType
from telethon import utils
from telethon.crypto import AuthKey
from telethon.tl.types import (
    InputPhoto, InputDocument, PeerUser, PeerChat, PeerChannel, updates
)

LATEST_VERSION = 2


class AlchemySessionContainer:
    def __init__(self, engine=None, session: orm.Session = None, table_prefix: str = "",
                 table_base: Optional[declarative_base] = None, manage_tables: bool = True) -> None:
        if isinstance(engine, str):
            engine = sql.create_engine(engine)

        self.db_engine = engine
        if not session:
            db_factory = orm.sessionmaker(bind=self.db_engine)
            self.db = orm.scoping.scoped_session(db_factory)
        else:
            self.db = session

        table_base = table_base or declarative_base()
        (self.Version, self.Session, self.Entity,
         self.SentFile, self.UpdateState) = self.create_table_classes(self.db, table_prefix,
                                                                      table_base)
        self.core_mode = False

        if manage_tables:
            table_base.metadata.bind = self.db_engine
            if not self.db_engine.dialect.has_table(self.db_engine,
                                                    self.Version.__tablename__):
                table_base.metadata.create_all()
                self.db.add(self.Version(version=LATEST_VERSION))
                self.db.commit()
            else:
                self.check_and_upgrade_database()

    @staticmethod
    def create_table_classes(db, prefix: str, base: declarative_base
                             ) -> Tuple[Any, Any, Any, Any, Any]:
        class Version(base):
            query = db.query_property()
            __tablename__ = "{prefix}version".format(prefix=prefix)
            version = Column(Integer, primary_key=True)

            def __str__(self):
                return "Version('{}')".format(self.version)

        class Session(base):
            query = db.query_property()
            __tablename__ = '{prefix}sessions'.format(prefix=prefix)

            session_id = Column(String(255), primary_key=True)
            dc_id = Column(Integer, primary_key=True)
            server_address = Column(String(255))
            port = Column(Integer)
            auth_key = Column(LargeBinary)

            def __str__(self):
                return "Session('{}', {}, '{}', {}, {})".format(self.session_id, self.dc_id,
                                                                self.server_address, self.port,
                                                                self.auth_key)

        class Entity(base):
            query = db.query_property()
            __tablename__ = '{prefix}entities'.format(prefix=prefix)

            session_id = Column(String(255), primary_key=True)
            id = Column(BigInteger, primary_key=True)
            hash = Column(BigInteger, nullable=False)
            username = Column(String(32))
            phone = Column(BigInteger)
            name = Column(String(255))

            def __str__(self):
                return "Entity('{}', {}, {}, '{}', '{}', '{}')".format(self.session_id, self.id,
                                                                       self.hash, self.username,
                                                                       self.phone, self.name)

        class SentFile(base):
            query = db.query_property()
            __tablename__ = '{prefix}sent_files'.format(prefix=prefix)

            session_id = Column(String(255), primary_key=True)
            md5_digest = Column(LargeBinary, primary_key=True)
            file_size = Column(Integer, primary_key=True)
            type = Column(Integer, primary_key=True)
            id = Column(BigInteger)
            hash = Column(BigInteger)

            def __str__(self):
                return "SentFile('{}', {}, {}, {}, {}, {})".format(self.session_id,
                                                                   self.md5_digest, self.file_size,
                                                                   self.type, self.id, self.hash)

        class UpdateState(base):
            query = db.query_property()
            __tablename__ = "{prefix}update_state".format(prefix=prefix)

            session_id = Column(String(255), primary_key=True)
            entity_id = Column(BigInteger, primary_key=True)
            pts = Column(BigInteger)
            qts = Column(BigInteger)
            date = Column(BigInteger)
            seq = Column(BigInteger)
            unread_count = Column(Integer)

        return Version, Session, Entity, SentFile, UpdateState

    def _add_column(self, table: Any, column: Column) -> None:
        column_name = column.compile(dialect=self.db_engine.dialect)
        column_type = column.type.compile(self.db_engine.dialect)
        self.db_engine.execute("ALTER TABLE {} ADD COLUMN {} {}".format(
            table.__tablename__, column_name, column_type))

    def check_and_upgrade_database(self) -> None:
        row = self.Version.query.all()
        version = row[0].version if row else 1
        if version == LATEST_VERSION:
            return

        self.Version.query.delete()

        if version == 1:
            self.UpdateState.__table__.create(self.db_engine)
            version = 3
        elif version == 2:
            self._add_column(self.UpdateState, Column(type=Integer, name="unread_count"))

        self.db.add(self.Version(version=version))
        self.db.commit()

    def new_session(self, session_id: str) -> 'AlchemySession':
        if self.core_mode:
            return AlchemyCoreSession(self, session_id)
        return AlchemySession(self, session_id)

    def list_sessions(self):
        return

    def save(self) -> None:
        self.db.commit()


class AlchemySession(MemorySession):
    def __init__(self, container: AlchemySessionContainer, session_id: str) -> None:
        super().__init__()
        self.container = container
        self.db = container.db
        self.engine = container.db_engine
        self.Version, self.Session, self.Entity, self.SentFile, self.UpdateState = (
            container.Version, container.Session, container.Entity,
            container.SentFile, container.UpdateState)
        self.session_id = session_id
        self._load_session()

    def _load_session(self) -> None:
        sessions = self._db_query(self.Session).all()
        session = sessions[0] if sessions else None
        if session:
            self._dc_id = session.dc_id
            self._server_address = session.server_address
            self._port = session.port
            self._auth_key = AuthKey(data=session.auth_key)

    def clone(self, to_instance=None) -> MemorySession:
        return super().clone(MemorySession())

    def set_dc(self, dc_id: str, server_address: str, port: int) -> None:
        super().set_dc(dc_id, server_address, port)
        self._update_session_table()

        sessions = self._db_query(self.Session).all()
        session = sessions[0] if sessions else None
        if session and session.auth_key:
            self._auth_key = AuthKey(data=session.auth_key)
        else:
            self._auth_key = None

    def get_update_state(self, entity_id: int) -> Optional[updates.State]:
        row = self.UpdateState.query.get((self.session_id, entity_id))
        if row:
            date = datetime.datetime.utcfromtimestamp(row.date)
            return updates.State(row.pts, row.qts, date, row.seq, row.unread_count)
        return None

    def set_update_state(self, entity_id: int, row: Any) -> None:
        if row:
            self.db.merge(self.UpdateState(session_id=self.session_id, entity_id=entity_id,
                                           pts=row.pts, qts=row.qts, date=row.date.timestamp(),
                                           seq=row.seq,
                                           unread_count=row.unread_count))
            self.save()

    @MemorySession.auth_key.setter
    def auth_key(self, value: AuthKey) -> None:
        self._auth_key = value
        self._update_session_table()

    def _update_session_table(self) -> None:
        self.Session.query.filter(self.Session.session_id == self.session_id).delete()
        self.db.add(self.Session(session_id=self.session_id, dc_id=self._dc_id,
                                 server_address=self._server_address, port=self._port,
                                 auth_key=(self._auth_key.key if self._auth_key else b'')))

    def _db_query(self, dbclass: Any, *args: Any) -> orm.Query:
        return dbclass.query.filter(
            dbclass.session_id == self.session_id, *args
        )

    def save(self) -> None:
        self.container.save()

    def close(self) -> None:
        # Nothing to do here, connection is managed by AlchemySessionContainer.
        pass

    def delete(self) -> None:
        self._db_query(self.Session).delete()
        self._db_query(self.Entity).delete()
        self._db_query(self.SentFile).delete()
        self._db_query(self.UpdateState).delete()

    def _entity_values_to_row(self, id: int, hash: int, username: str, phone: str, name: str
                              ) -> Any:
        return self.Entity(session_id=self.session_id, id=id, hash=hash,
                           username=username, phone=phone, name=name)

    def process_entities(self, tlo: Any) -> None:
        rows = self._entities_to_rows(tlo)
        if not rows:
            return

        for row in rows:
            self.db.merge(row)
        self.save()

    def get_entity_rows_by_phone(self, key: str) -> Optional[Tuple[int, int]]:
        row = self._db_query(self.Entity,
                             self.Entity.phone == key).one_or_none()
        return (row.id, row.hash) if row else None

    def get_entity_rows_by_username(self, key: str) -> Optional[Tuple[int, int]]:
        row = self._db_query(self.Entity,
                             self.Entity.username == key).one_or_none()
        return (row.id, row.hash) if row else None

    def get_entity_rows_by_name(self, key: str) -> Optional[Tuple[int, int]]:
        row = self._db_query(self.Entity,
                             self.Entity.name == key).one_or_none()
        return (row.id, row.hash) if row else None

    def get_entity_rows_by_id(self, key: int, exact: bool = True) -> Optional[Tuple[int, int]]:
        if exact:
            query = self._db_query(self.Entity, self.Entity.id == key)
        else:
            ids = (
                utils.get_peer_id(PeerUser(key)),
                utils.get_peer_id(PeerChat(key)),
                utils.get_peer_id(PeerChannel(key))
            )
            query = self._db_query(self.Entity, self.Entity.id.in_(ids))

        row = query.one_or_none()
        return (row.id, row.hash) if row else None

    def get_file(self, md5_digest: str, file_size: int, cls: Any) -> Optional[Tuple[int, int]]:
        row = self._db_query(self.SentFile,
                             self.SentFile.md5_digest == md5_digest,
                             self.SentFile.file_size == file_size,
                             self.SentFile.type == _SentFileType.from_type(
                                 cls).value).one_or_none()
        return (row.id, row.hash) if row else None

    def cache_file(self, md5_digest: str, file_size: int,
                   instance: Union[InputDocument, InputPhoto]) -> None:
        if not isinstance(instance, (InputDocument, InputPhoto)):
            raise TypeError("Cannot cache {} instance".format(type(instance)))

        self.db.merge(
            self.SentFile(session_id=self.session_id, md5_digest=md5_digest, file_size=file_size,
                          type=_SentFileType.from_type(type(instance)).value,
                          id=instance.id, hash=instance.access_hash))
        self.save()


class AlchemyCoreSession(AlchemySession):
    def _load_session(self) -> None:
        t = self.Session.__table__
        rows = self.engine.execute(select([t.c.dc_id, t.c.server_address, t.c.port, t.c.auth_key])
                                   .where(t.c.session_id == self.session_id))
        try:
            self._dc_id, self._server_address, self._port, auth_key = next(rows)
            self._auth_key = AuthKey(data=auth_key)
        except StopIteration:
            pass

    def get_update_state(self, entity_id: int) -> Optional[updates.State]:
        t = self.UpdateState.__table__
        rows = self.engine.execute(select([t])
                                   .where(and_(t.c.session_id == self.session_id,
                                               t.c.entity_id == entity_id)))
        try:
            _, _, pts, qts, date, seq, unread_count = next(rows)
            date = datetime.datetime.utcfromtimestamp(date)
            return updates.State(pts, qts, date, seq, unread_count)
        except StopIteration:
            return None

    def set_update_state(self, entity_id: int, row: Any) -> None:
        t = self.UpdateState.__table__
        values = dict(pts=row.pts, qts=row.qts, date=row.date.timestamp(),
                      seq=row.seq, unread_count=row.unread_count)
        self.engine.execute(insert(t)
                            .values(session_id=self.session_id, entity_id=entity_id, **values)
                            .on_conflict_do_update(constraint=t.primary_key, set_=values))

    def _update_session_table(self) -> None:
        self.engine.execute(
            self.Session.__table__.delete().where(self.Session.session_id == self.session_id))
        self.engine.execute(insert(self.Session.__table__),
                            session_id=self.session_id, dc_id=self._dc_id,
                            server_address=self._server_address, port=self._port,
                            auth_key=(self._auth_key.key if self._auth_key else b''))

    def save(self) -> None:
        # engine.execute() autocommits
        pass

    def delete(self) -> None:
        self.engine.execute(self.Session.__table__.delete().where(
            self.Session.__table__.c.session_id == self.session_id))
        self.engine.execute(self.Entity.__table__.delete().where(
            self.Entity.__table__.c.session_id == self.session_id))
        self.engine.execute(self.SentFile.__table__.delete().where(
            self.SentFile.__table__.c.session_id == self.session_id))
        self.engine.execute(self.UpdateState.__table__.delete().where(
            self.UpdateState.__table__.c.session_id == self.session_id))

    def _entity_values_to_row(self, id: int, hash: int, username: str, phone: str, name: str
                              ) -> Any:
        return id, hash, username, phone, name

    def process_entities(self, tlo: Any) -> None:
        rows = self._entities_to_rows(tlo)
        if not rows:
            return

        t = self.Entity.__table__
        with self.engine.begin() as conn:
            for row in rows:
                values = dict(hash=row[1], username=row[2], phone=row[3], name=row[4])
                conn.execute(insert(t)
                             .values(session_id=self.session_id, id=row[0], **values)
                             .on_conflict_do_update(constraint=t.primary_key, set_=values))

    def get_entity_rows_by_phone(self, key: str) -> Optional[Tuple[int, int]]:
        t = self.Entity.__table__
        rows = self.engine.execute(select([t.c.id, t.c.hash]).where(
            and_(t.c.session_id == self.session_id, t.c.phone == key)))
        try:
            return next(rows)
        except StopIteration:
            return None

    def get_entity_rows_by_username(self, key: str) -> Optional[Tuple[int, int]]:
        t = self.Entity.__table__
        rows = self.engine.execute(select([t.c.id, t.c.hash]).where(
            and_(t.c.session_id == self.session_id, t.c.username == key)))
        try:
            return next(rows)
        except StopIteration:
            return None

    def get_entity_rows_by_name(self, key: str) -> Optional[Tuple[int, int]]:
        t = self.Entity.__table__
        rows = self.engine.execute(select([t.c.id, t.c.hash])
                                   .where(and_(t.c.session_id == self.session_id, t.c.name == key)))
        try:
            return next(rows)
        except StopIteration:
            return None

    def get_entity_rows_by_id(self, key: int, exact: bool = True) -> Optional[Tuple[int, int]]:
        t = self.Entity.__table__
        if exact:
            rows = self.engine.execute(select([t.c.id, t.c.hash]).where(
                and_(t.c.session_id == self.session_id, t.c.id == key)))
        else:
            ids = (
                utils.get_peer_id(PeerUser(key)),
                utils.get_peer_id(PeerChat(key)),
                utils.get_peer_id(PeerChannel(key))
            )
            rows = self.engine.execute(select([t.c.id, t.c.hash])
                .where(
                and_(t.c.session_id == self.session_id, t.c.id.in_(ids))))

        try:
            return next(rows)
        except StopIteration:
            return None

    def get_file(self, md5_digest: str, file_size: int, cls: Any) -> Optional[Tuple[int, int]]:
        t = self.SentFile.__table__
        rows = (self.engine.execute(select([t.c.id, t.c.hash])
                                    .where(and_(t.c.session_id == self.session_id,
                                                t.c.md5_digest == md5_digest,
                                                t.c.file_size == file_size,
                                                t.c.type == _SentFileType.from_type(cls).value))))
        try:
            return next(rows)
        except StopIteration:
            return None

    def cache_file(self, md5_digest: str, file_size: int,
                   instance: Union[InputDocument, InputPhoto]) -> None:
        if not isinstance(instance, (InputDocument, InputPhoto)):
            raise TypeError("Cannot cache {} instance".format(type(instance)))

        t = self.SentFile.__table__
        values = dict(id=instance.id, hash=instance.access_hash)
        self.engine.execute(insert(t)
                            .values(session_id=self.session_id, md5_digest=md5_digest,
                                    type=_SentFileType.from_type(type(instance)).value,
                                    file_size=file_size, **values)
                            .on_conflict_do_update(constraint=t.primary_key, set_=values))
