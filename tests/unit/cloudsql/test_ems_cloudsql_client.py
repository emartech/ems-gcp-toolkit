from unittest import TestCase
from unittest.mock import patch

from googleapiclient import discovery

from cloudsql.ems_cloudsql_client import EmsCloudsqlClient


class TestEmsCloudsqlClient(TestCase):

    # pylint:disable=unused-argument
    @patch("cloudsql.ems_cloudsql_client.discovery")
    def test_properties(self, discovery_module_patch: discovery):
        client = EmsCloudsqlClient("some-project-id", "some-instance-id")

        self.assertEqual(client.project_id, "some-project-id")
        self.assertEqual(client.instance_id, "some-instance-id")
