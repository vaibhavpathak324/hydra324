import argparse
import logging
import os

import uvicorn


def main() -> None:
    parser = argparse.ArgumentParser(prog="hydra")
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=int(os.environ.get("PORT", "8000")))
    parser.add_argument(
        "--bot-token",
        default=os.environ.get("HYDRA_BOT_TOKEN") or os.environ.get("BOT_TOKEN") or "",
        help="Telegram control-bot token. Can also be set later in the panel.",
    )
    args = parser.parse_args()
    if args.bot_token:
        os.environ["HYDRA_BOT_TOKEN"] = args.bot_token
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    uvicorn.run(
        "hydra.server:app",
        host=args.host,
        port=args.port,
        reload=False,
        proxy_headers=True,
        forwarded_allow_ips="*",
    )


if __name__ == "__main__":
    main()
