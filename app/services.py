from common import *
import requests
from circuit_breaker import CircuitBreaker, CircuitOpenException
from functools import wraps


def wrap_cb(service):

    def wrap_cb(fn):
        cb = CircuitBreaker(service, failure_threshold=3, recovery_timeout=20)

        @wraps(fn)
        def wrapper(*args, **kwargs):
            return cb.call(fn, *args, **kwargs)

        return wrapper

    return wrap_cb


class FlightsService:
    NAME = "Flights Service"
    
    def __init__(self, url):
        self.url = url

    def healthcheck(self):
        response = requests.get(f"{self.url}/manage/health")
        response.raise_for_status()

    @wrap_cb(NAME)
    def get_all(self, page: int = None, size: int = None):
        response = requests.get(
            f"{self.url}/flights", params={"page": page, "size": size}
        )
        response.raise_for_status()
        return PaginationResponse.model_validate(response.json())

    @wrap_cb(NAME)
    def get_flight_by_number(self, flight_number: str) -> FlightResponse:
        response = requests.get(f"{self.url}/flights/{flight_number}")
        response.raise_for_status()
        return FlightResponse.model_validate(response.json())

    def get_flight_by_number_or_default(self, flight_number: str) -> FlightResponse:
        try:
            return self.get_flight_by_number(flight_number)
        except CircuitOpenException:
            return FlightResponse(
                flightNumber="XXX",
                fromAirport="XXX",
                toAirport="XXX",
                date=datetime.fromordinal(1),
                price=0,
            )


class TicketsService:
    NAME = "Ticket Service"
    
    def __init__(self, url):
        self.url = url

    def healthcheck(self):
        response = requests.get(f"{self.url}/manage/health")
        response.raise_for_status()

    @wrap_cb(NAME)
    def get_user_tickets(self, username) -> list[Ticket]:
        response = requests.get(f"{self.url}/tickets/user/{username}")
        response.raise_for_status()
        return [Ticket.model_validate(x) for x in response.json()]

    @wrap_cb(NAME)
    def get_ticket(self, ticket_uid) -> Ticket | None:
        response = requests.get(f"{self.url}/tickets/{ticket_uid}")
        if response.status_code == 404:
            return None
        response.raise_for_status()
        return Ticket.model_validate(response.json())

    def delete_ticket(self, ticket_uid) -> None:
        response = requests.delete(f"{self.url}/tickets/{ticket_uid}")
        response.raise_for_status()

    def create_ticket(self, ticket_uid, username, flight_number, price):
        response = requests.post(
            f"{self.url}/tickets",
            json=TicketCreateRequest(
                ticketUid=ticket_uid,
                username=username,
                flightNumber=flight_number,
                price=price,
            ).model_dump(mode="json"),
        )
        response.raise_for_status()


class PrivilegesService:
    NAME = "Bonus Service"
    
    def __init__(self, url):
        self.url = url

    def healthcheck(self):
        response = requests.get(f"{self.url}/manage/health")
        response.raise_for_status()

    @wrap_cb(NAME)
    def get_user_privelge(self, username) -> Privilege:
        response = requests.get(f"{self.url}/privilege/{username}")
        if response.status_code == 404:
            return None
        response.raise_for_status()
        privilege = Privilege.model_validate(response.json())
        return privilege

    @wrap_cb(NAME)
    def get_user_privelge_history(self, username) -> list[PrivilegeHistory]:
        response = requests.get(f"{self.url}/privilege/{username}/history")
        response.raise_for_status()
        return [PrivilegeHistory.model_validate(x) for x in response.json()]

    @wrap_cb(NAME)
    def get_user_privelge_transaction(self, username, ticket_uid) -> PrivilegeHistory:
        response = requests.get(f"{self.url}/privilege/{username}/history/{ticket_uid}")
        if response.status_code == 404:
            return None
        response.raise_for_status()
        return PrivilegeHistory.model_validate(response.json())

    def add_transaction(self, username, data: AddTranscationRequest):
        response = requests.post(
            f"{self.url}/privilege/{username}/history",
            json=data.model_dump(mode="json"),
        )
        response.raise_for_status()

    def rollback_transaction(self, username, ticket_uid):
        response = requests.delete(
            f"{self.url}/privilege/{username}/history/{ticket_uid}"
        )
        response.raise_for_status()
