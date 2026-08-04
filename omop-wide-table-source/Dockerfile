FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir uv

# sdk/ is the committed copy of the upstream sdk's py-sdk (the node builder
# clones without submodules, so the build can't see sdk-upstream/). Refresh it
# with ./sync-sdk.sh, never by hand.
COPY sdk/ ./sdk/
# Scrub any build artifacts before installing: setuptools reuses a stale
# build/lib as the wheel staging dir and silently packages old code.
RUN rm -rf ./sdk/build ./sdk/dist ./sdk/src/*.egg-info \
    && uv pip install --system --no-cache ./sdk/ boto3 requests==2.32.3

COPY transform.py ./

CMD ["python", "transform.py"]
