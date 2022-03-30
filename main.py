import sync

tables_to_sync = ['github_profile', 'github_profile', 'github_profile']
batch_size = 5000
sync.sync(tables_to_sync, batch_size)
