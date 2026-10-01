"""Outbox model schema tests — manifest_id cascades on manifest delete."""

from app.models.outbox import Outbox


class TestOutboxManifestIdForeignKey:
    def test_manifest_id_is_nullable(self):
        column = Outbox.__table__.c.manifest_id
        assert column.nullable is True

    def test_fk_ondelete_is_cascade(self):
        fks = list(Outbox.__table__.c.manifest_id.foreign_keys)
        assert len(fks) == 1
        fk = fks[0]
        assert fk.target_fullname == "manifest_tasks.id"
        assert fk.ondelete == "CASCADE"
        assert fk.name == "outbox_manifest_id_fkey"

    def test_no_duplicate_foreign_key_constraints(self):
        fk_constraints = [
            c
            for c in Outbox.__table__.constraints
            if c.__class__.__name__ == "ForeignKeyConstraint"
        ]
        assert len(fk_constraints) == 1
