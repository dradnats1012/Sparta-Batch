from service.road_address_update_service import run_sync_batch

if __name__ == "__main__":
    run_sync_batch(batch_size=100)