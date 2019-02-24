from typing import Any, Union

from sqlalchemy.dialects.mysql import insert

from telethon.sessions.memory import _SentFileType
from telethon.tl.types import InputPhoto, InputDocument

from .core import AlchemyCoreSession


class AlchemyMySQLCoreSession(AlchemyCoreSession):
    def set_update_state(self, entity_id: int, row: Any) -> None:
        t = self.UpdateState.__table__
        values = dict(pts=row.pts, qts=row.qts, date=row.date.timestamp(),
                      seq=row.seq, unread_count=row.unread_count)
        with self.engine.begin() as conn:
            conn.execute(insert(t)
                         .values(session_id=self.session_id, entity_id=entity_id, **values)
                         .on_duplicate_key_update(**values))

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
                             .on_duplicate_key_update(**values))

    def cache_file(self, md5_digest: str, file_size: int,
                   instance: Union[InputDocument, InputPhoto]) -> None:
        if not isinstance(instance, (InputDocument, InputPhoto)):
            raise TypeError("Cannot cache {} instance".format(type(instance)))

        t = self.SentFile.__table__
        values = dict(id=instance.id, hash=instance.access_hash)
        with self.engine.begin() as conn:
            conn.execute(insert(t)
                         .values(session_id=self.session_id, md5_digest=md5_digest,
                                 type=_SentFileType.from_type(type(instance)).value,
                                 file_size=file_size, **values)
                         .on_duplicate_key_update(**values))
