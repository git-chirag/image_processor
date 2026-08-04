from app.config import REQUEST_TTL_SECONDS, redis_client


def request_key(request_id, field):
    return f"request:{request_id}:{field}"


def set_request_value(request_id, field, value, *, nx=False):
    key = request_key(request_id, field)
    was_set = redis_client.set(
        key,
        value,
        ex=REQUEST_TTL_SECONDS,
        nx=nx,
    )
    if nx and not was_set:
        redis_client.expire(key, REQUEST_TTL_SECONDS)
    return was_set


def expire_request_key(key):
    redis_client.expire(key, REQUEST_TTL_SECONDS)
