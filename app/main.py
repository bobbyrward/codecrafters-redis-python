from dataclasses import dataclass
from typing import List
import asyncio


@dataclass
class Command:
    command: str
    args: list


async def read_bulk_string(reader):
    length = int((await reader.readuntil(separator=b"\r\n"))[:2])
    value = await reader.read(length)
    await reader.read(2)
    return value


async def read_simple_string(reader):
    (await reader.readuntil(separator=b"\r\n"))[:2]


async def read_error_string(reader):
    (await reader.readuntil(separator=b"\r\n"))[:2]


async def read_integer(reader):
    int((await reader.readuntil(separator=b"\r\n"))[:2])


async def read_type(reader):
    resp_type = await reader.read(1)

    type_map = {
        b"*": read_array,
        b"$": read_bulk_string,
        b"+": read_simple_string,
        b"-": read_error_string,
        b":": read_integer,
    }

    try:
        type_reader = type_map[resp_type]
    except KeyError:
        raise Exception(f"Unknown RESP type: {resp_type}")

    return await type_reader(reader)


async def read_array(reader):
    array_len = int((await reader.readuntil(separator=b"\r\n"))[:2])
    parts = []

    for index in range(array_len):
        array_part = await read_type(reader)

        parts.append(array_part)

    return parts


async def read_command(reader):
    resp_type = await reader.read(1)
    if not resp_type:
        return None

    if resp_type != b"*":
        raise Exception(f"commands should be arrays not {resp_type}")

    command_array = await read_array(reader)

    return Command(command=command_array[0], args=command_array[1:])


async def handle_ping(writer, args):
    response = "PONG"

    if not args:
        response = b"PONG"
    elif len(args) == 1:
        response = args[0]
    else:
        raise Exception(f"PING has wrong arguments: {args}")

    writer.write(b"".join((b"+", response, b"\r\n")))


async def handle_echo(writer, args):
    if len(args) != 1:
        raise Exception(f"ECHO has wrong arguments: {args}")

    writer.write(b"".join((b"+", args[0], b"\r\n")))


COMMAND_MAP = {
    b"PING": handle_ping,
    b"ECHO": handle_echo,
}


async def handle_connection(reader, writer):
    while True:
        command = await read_command(reader)

        if not command:
            print("Client disconnected")
            break

        print(f"COMMAND = '{command}'")

        try:
            command_handler = COMMAND_MAP[command.command]
        except KeyError:
            print(f"Unknown command: {command.command}")
            break

        await command_handler(writer, command.args)


def main():
    loop = asyncio.get_event_loop()
    server = loop.run_until_complete(
        asyncio.start_server(handle_connection, host="localhost", port=6379, reuse_address=True)
    )

    try:
        loop.run_forever()
    except KeyboardInterrupt:
        pass

    # Close the server
    server.close()
    loop.run_until_complete(server.wait_closed())
    loop.close()


if __name__ == "__main__":
    main()
