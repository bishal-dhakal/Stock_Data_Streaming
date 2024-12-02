from __future__ import absolute_import
from logging.config import fileConfig

from sqlalchemy import engine_from_config, pool
from alembic import context

from core.database import Base  # Adjust to your project structure
from app.models import *  # Import all your models here

# This is the Alembic Config object, which provides access to the values within the .ini file in use.
config = context.config

# Interpret the config file for Python logging.
fileConfig(config.config_file_name)

# Add your model's MetaData object here
target_metadata = Base.metadata

def run_migrations_offline():
    # Offline migration code
    url = config.get_main_option("sqlalchemy.url")
    context.configure(url=url, target_metadata=target_metadata)

    with context.begin_transaction():
        context.run_migrations()

def run_migrations_online():
    # Online migration code
    connectable = engine_from_config(config.get_section(config.config_ini_section),
                                     prefix='sqlalchemy.',
                                     poolclass=pool.NullPool)

    with connectable.connect() as connection:
        context.configure(connection=connection, target_metadata=target_metadata)

        with context.begin_transaction():
            context.run_migrations()

if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
