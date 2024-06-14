import faker
from faker.providers import address, misc
import csv
import os

fake = faker.Faker('id_ID')

tables = {
    't_invoice_h': [],
    't_invoice_d': [],
    't_so_h': [],
    't_so_d':[],
    'dopenjualan_t_co_h': [],
    'dopenjualan_t_co_d': [],
    'm_barang': [],
    'm_satuan': [],
    'm_pelanggan': []
}

for _ in range(1000):
    tanggal_inv = fake.date_between(start_date="-1y", end_date="today")

    pelanggan_id = fake.pystr(max_chars=8)
    barang_id = fake.pystr(max_chars=6)
    invoice_id = fake.pystr(max_chars=8)
    so_id = fake.pystr(max_chars=8)
    co_id = fake.pystr(max_chars=8)
    satuan_id = fake.pyint(min_value=10, max_value=99)

    t_invoice_h_data = {
        "id": invoice_id,
        "tanggal_inv": tanggal_inv,
        "due_date": fake.date_between(start_date=tanggal_inv, end_date="+30d"),
        "pelanggan_id": pelanggan_id,
        "nomor_co": fake.pystr(max_chars=8),
        "nilai_inv": fake.pyfloat(left_digits=5, right_digits=2),
        "gross_inv": fake.pyfloat(left_digits=5, right_digits=2),
        "disc_inv": fake.pyfloat(left_digits=1, right_digits=2),
        "sudah_dibayar": fake.random_element(elements=['Y', 'N']),
        "suratjalan_id": fake.pystr(max_chars=8),
        "created_at": fake.date_time(),
        "updated_at": fake.date_time(),
    }

    m_barang_data = {
        "id": barang_id,
        "nama": fake.word(),
        "satuan_id": satuan_id,
        "harga": fake.pyfloat(left_digits=4, right_digits=2),
        "qty_stok": fake.pyint(min_value=100, max_value=999),
        "qty_booking": fake.pyint(min_value=10, max_value=99),
        "created_at": fake.date_time(),
        "updated_at": fake.date_time(),
    }

    t_invoice_d_data = {
        "inv_id": invoice_id,
        "barang_id": barang_id,
        "qfy": fake.random_int(min=1, max=10),
        "harga_barang": fake.pyfloat(left_digits=4, right_digits=2),
        "disc": fake.pyfloat(left_digits=1, right_digits=2),
    }

    t_so_h_data = {
        "id": so_id,
        "tanggal_so": fake.date_between(start_date="-1y", end_date="today"),
        "nomor_po": fake.pystr(max_chars=20),
        "pelanggan_id": pelanggan_id,
        "created_at": fake.date_time(),
        "updated_at": fake.date_time(),
    }

    t_so_d_data = {
        "id" : so_id,
        "barang_id": barang_id,
        "qty": fake.random_int(min=1, max=10),
    }


    dopenjualan_t_co_h_data = {
        "id": fake.pystr(max_chars=8),
        "pelanggan_id": t_so_h_data["pelanggan_id"],
        "tanggal_co": t_so_h_data["tanggal_so"],
        "nomor_so": t_so_h_data["id"],
        "created_at": fake.date_time(),
        "updated_at": fake.date_time(),
    }

    dopenjualan_t_co_d_data = {
        "co_id": co_id,
        "barang_id": barang_id,
        "qly": t_invoice_d_data["qfy"],
    }

    m_satuan_data = {
        "id": satuan_id,
        "nama": fake.word(),
        "created_at": fake.date_time(),
        "updated_at": fake.date_time(),
    }

    m_pelanggan_data = {
        "id": pelanggan_id,
        "nama": fake.name(),
        "alamat":fake.address(),
        "no_telp":fake.phone_number(),
        "email":fake.email(),
        "top":fake.pyfloat(),
        "disc": fake.pyfloat(left_digits=1, right_digits=2),
        "created_at": fake.date_time(),
        "updated_at": fake.date_time(),
    }
    
    tables['t_invoice_h'].append(t_invoice_h_data)
    tables['t_invoice_d'].append(t_invoice_d_data)
    tables['t_so_h'].append(t_so_h_data)
    tables['t_so_d'].append(t_so_d_data)
    tables['dopenjualan_t_co_h'].append(dopenjualan_t_co_h_data)
    tables['dopenjualan_t_co_d'].append(dopenjualan_t_co_d_data)
    tables['m_barang'].append(m_barang_data)
    tables['m_satuan'].append(m_satuan_data)
    tables['m_pelanggan'].append(m_pelanggan_data)

output_dir = 'dummy_data3'

os.makedirs(output_dir, exist_ok=True)

# Optional: Clean up any existing files in the output directory
for filename in os.listdir(output_dir):
    file_path = os.path.join(output_dir, filename)
    try:
        if os.path.isfile(file_path):
            os.unlink(file_path)
    except Exception as e:
        print(f'Failed to delete {file_path}. Reason: {e}')

for table, data in tables.items():
    csv_file_path = os.path.join(output_dir, f'{table}.csv')
    with open(csv_file_path, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=data[0].keys())
        writer.writeheader()
        for row in data:
            writer.writerow(row)
