"""Example entity test -- validates the customers entity with sample data.

Run with: fyr test  (or: pytest tests/)
"""

from entities.customers import entity as customers_entity
from fyrnheim.testing import EntityTest


class TestCustomers(EntityTest):
    entity = customers_entity

    def test_basic_transform(self):
        """Verify that sample customer data flows through the pipeline."""
        result = (
            self.given(
                {
                    "source_customers": [
                        {
                            "id": 1,
                            "name": "Alice",
                            "email": "alice@example.com",
                            "plan": "pro",
                            "amount_cents": 4900,
                            "created_at": "2024-01-15",
                        },
                        {
                            "id": 2,
                            "name": "Bob",
                            "email": "bob@test.org",
                            "plan": "free",
                            "amount_cents": 0,
                            "created_at": "2024-02-20",
                        },
                    ]
                }
            )
            .run()
        )

        assert result.row_count == 2
        assert "email_hash" in result.columns
        assert "amount_dollars" in result.columns
