import random
from faker import Faker

# Instância do Faker
fake = Faker('pt_BR')


def generate_csv_line(id):
    nome = fake.name()
    email = fake.email()
    cidade = fake.city()
    return f'{id},{nome},{email},{cidade}'


def generate_csv(num_lines=50):
    header = 'id,nome,email,cidade'
    lines = [header]
    for i in range(1, num_lines + 1):
        lines.append(generate_csv_line(i))
    return '\n'.join(lines)


# Gerar a string CSV
csv_string = generate_csv()
print(csv_string)
