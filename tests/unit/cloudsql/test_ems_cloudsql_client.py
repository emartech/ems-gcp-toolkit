from unittest import TestCase
from unittest.mock import patch

from cloudsql.ems_cloudsql_client import EmsCloudsqlClient


class TestEmsCloudsqlClient(TestCase):

    @patch("cloudsql.ems_cloudsql_client.discovery")
    def test_properties(self):
        client = EmsCloudsqlClient("some-project-id", "some-instance-id")

        self.assertEqual(client.project_id, "some-project-id")
        self.assertEqual(client.instance_id, "some-instance-id")
