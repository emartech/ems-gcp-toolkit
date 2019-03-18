from unittest import TestCase

from cloudsql.ems_cloudsql_client import EmsCloudsqlClient


class TestEmsCloudsqlClient(TestCase):

    def test_properties(self):
        client = EmsCloudsqlClient("some-project-id", "some-instance-id")

        self.assertEqual(client.project_id, "some-project-id")
        self.assertEqual(client.instance_id, "some-instance-id")
