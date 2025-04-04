from prefect import flow, task
import random

@task
def get_customer_ids():
    # Fetch customer IDs from a database or API
    return [f"customer{n}" for n in random.choices(range(100), k=50)]

@task
def process_customer(customer_id: str) -> str:
    # Process a single customer
    return f"Processed {customer_id}"

@flow
def customer_processing_flow() -> list[str]:
    customer_ids = get_customer_ids()
    # Map the process_customer task across all customer IDs
    results = process_customer.map(customer_ids)
    return results


if __name__ == "__main__":
    customer_processing_flow()
