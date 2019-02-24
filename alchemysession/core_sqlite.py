from typing import Any, Union

from telethon.sessions.memory import _SentFileType
from telethon.tl.types import InputPhoto, InputDocument

from .core import AlchemyCoreSession


class AlchemySQLiteCoreSession(AlchemyCoreSession):
    def set_update_state(self, entity_id: int, row: Any) -> None:
        with self.engine.begin() as conn:
            conn.execute("INSERT OR REPLACE INTO {} ".format(self.UpdateState.__tablename__) +
                         "VALUES (:session_id, :entity_id, :pts, :qts, :date, :seq, "
                         "        :unread_count)",
                         dict(session_id=self.session_id, entity_id=entity_id, pts=row.pts,
                              qts=row.qts, date=row.date.timestamp(), seq=row.seq,
                              unread_count=row.unread_count))

    def process_entities(self, tlo: Any) -> None:
        rows = self._entities_to_rows(tlo)
        if not rows:
            return

        with self.engine.begin() as conn:
            conn.execute("INSERT OR REPLACE INTO {} ".format(self.Entity.__tablename__) +
                         "VALUES (:session_id, :id, :hash, :username, :phone, :name)",
                         [dict(session_id=self.session_id, id=row[0], hash=row[1],
                               username=row[2], phone=row[3], name=row[4])
                          for row in rows])

    def cache_file(self, md5_digest: str, file_size: int,
                   instance: Union[InputDocument, InputPhoto]) -> None:
        if not isinstance(instance, (InputDocument, InputPhoto)):
            raise TypeError("Cannot cache {} instance".format(type(instance)))

        t = self.SentFile.__table__
        values = dict(id=instance.id, hash=instance.access_hash)
        with self.engine.begin() as conn:
            conn.execute("INSERT OR REPLACE INTO {} ".format(self.SentFile.__tablename__) +
                         "VALUES (:session_id, :md5_digest, :type, :file_size)",
                         dict(session_id=self.session_id, md5_digest=md5_digest,
                              type=_SentFileType.from_type(type(instance)).value,
                              file_size=file_size, **values))
