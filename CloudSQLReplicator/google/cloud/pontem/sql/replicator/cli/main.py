# Copyright 2018 The Pontem Authors. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Cloud SQL Replicator CLI main."""

from __future__ import print_function

from builtins import input
import sys

from absl import app
from absl import flags
from absl import logging

from google.cloud.pontem.sql import replicator
from google.cloud.pontem.sql.replicator.util import cloudsql

FLAGS = flags.FLAGS

flags.DEFINE_bool('interactive',
                  False,
                  'Run replicator in interactive mode.',
                  short_name='i')


def usage():
    """Describes how the command line is used."""
    print('usage: main.py [-h | -i]')


def replicate_interactive():
    """Sets up replica interactively."""
    instance_name = input('Enter instance name: ').strip()
    ip_address = cloudsql.get_outgoing_ip_of_instance(instance_name)
    print('IP Address: {}'.format(ip_address))


def main(argv):
    """Main entry point for CLI."""
    # pylint: disable=missing-param-doc, missing-type-doc
    del argv  # Unused.

    logging.info('Running under Python {0[0]}.{0[1]}.{0[2]}'
                 .format(sys.version_info))
    logging.info('Running version {} of replicator'
                 .format(replicator.__version__))

    if FLAGS.interactive:
        replicate_interactive()
    else:
        app.usage(shorthelp=True)


if __name__ == '__main__':
    app.run(main)
