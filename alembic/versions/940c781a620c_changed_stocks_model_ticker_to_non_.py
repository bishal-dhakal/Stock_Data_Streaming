"""changed_stocks_model_ticker_to_non_unique

Revision ID: 940c781a620c
Revises: 1218c832d777
Create Date: 2024-12-02 18:31:35.496677

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '940c781a620c'
down_revision: Union[str, None] = '1218c832d777'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_index('ix_stocks_ticker', table_name='stocks')
    op.create_index(op.f('ix_stocks_ticker'), 'stocks', ['ticker'], unique=False)
    # ### end Alembic commands ###


def downgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_index(op.f('ix_stocks_ticker'), table_name='stocks')
    op.create_index('ix_stocks_ticker', 'stocks', ['ticker'], unique=True)
    # ### end Alembic commands ###
