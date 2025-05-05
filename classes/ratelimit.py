import asyncio
import time
from collections import defaultdict

# 100% ChatGPT
class RateLimiter:
    def __init__(self, auth_limit: int, interval: int, delay: int = 10):
        """
        :param auth_limit: Maximale Anzahl an falschen "auth" Versuchen pro IP
        :param interval: Zeitraum in Sekunden, nach dem die Sperren zurückgesetzt werden
        :param delay: Standard-Verzögerung in Sekunden für jede Anfrage
        """
        self.auth_limit = auth_limit
        self.interval = interval
        self.delay = delay

        self.auth_attempts = defaultdict(int)
        self.last_attempt_time = defaultdict(float)

        self.cleanup_task = None

    def start(self):
        self.cleanup_task = asyncio.create_task(self._clear_old_entries())

    def stop(self):
        self.cleanup_task.cancel()

    def is_allowed(self, ip: str) -> bool:
        """ Verzögert jede Anfrage. Blockiert Clients mit zu vielen falschen Auth-Versuchen. """

        if self.auth_attempts[ip] >= self.auth_limit:
            return False

        return True

    def register_auth_attempt(self, ip: str, success: bool):
        """ Registriert einen Authentifizierungsversuch. """
        self.last_attempt_time[ip] = time.time()

        if success:
            self.auth_attempts[ip] = 0
        else:
            self.auth_attempts[ip] += 1

    async def _clear_old_entries(self):
        """ Läuft im Hintergrund und löscht alle X Sekunden alte Einträge. """
        while True:
            await asyncio.sleep(self.interval)

            now = time.time()
            to_remove = [ip for ip, t in self.last_attempt_time.items() if now - t > self.interval]

            for ip in to_remove:
                self.auth_attempts.pop(ip, None)
                self.last_attempt_time.pop(ip, None)
