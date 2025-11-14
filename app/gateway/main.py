from fastapi import FastAPI, HTTPException, Header, BackgroundTasks
from fastapi.responses import JSONResponse
from pydantic import BaseModel
import os
import datetime
import uuid
import sys
import inspect

currentdir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
parentdir = os.path.dirname(currentdir)
sys.path.insert(0, parentdir)

from circuit_breaker import *
from common import *
from services import *

FLIGHTS_SERVICE_URL = os.getenv("FLIGHTS_SERVICE_URL")
TICKETS_SERVICE_URL = os.getenv("TICKETS_SERVICE_URL")
PRIVILEGES_SERVICE_URL = os.getenv("PRIVILEGES_SERVICE_URL")

if FLIGHTS_SERVICE_URL is None:
    raise RuntimeError("missing FLIGHTS_SERVICE_URL")
if TICKETS_SERVICE_URL is None:
    raise RuntimeError("missing TICKETS_SERVICE_URL")
if PRIVILEGES_SERVICE_URL is None:
    raise RuntimeError("missing PRIVILEGES_SERVICE_URL")

flights_service = FlightsService(FLIGHTS_SERVICE_URL)
tickets_service = TicketsService(TICKETS_SERVICE_URL)
privileges_service = PrivilegesService(PRIVILEGES_SERVICE_URL)


app = FastAPI(title="App API", root_path="/api/v1")


class TicketBuyBody(BaseModel):
    flightNumber: str
    price: int
    paidFromBalance: bool


def error_response(msg, code):
    return JSONResponse(
        content=ErrorResponse(message=msg).model_dump(), status_code=code
    )


@app.exception_handler(CircuitOpenException)
async def circuit_open_exception_handler(request, exc):
    return error_response(f"{exc.service} unavailable", 503)


@app.get("/flights", response_model=PaginationResponse)
def get_flights(page: int = None, size: int = None):
    return flights_service.get_all(page, size)


def map_ticket_to_ticket_response(tick):
    flight = flights_service.get_flight_by_number_or_default(tick.flight_number)
    return TicketResponse(
        ticketUid=tick.ticket_uid,
        flightNumber=tick.flight_number,
        fromAirport=flight.fromAirport,
        toAirport=flight.toAirport,
        date=flight.date,
        price=flight.price,
        status=tick.status,
    )


@app.get("/tickets")
def get_tickets(x_user_name: str = Header()) -> List[TicketResponse]:
    privilege = privileges_service.get_user_privelge(x_user_name)
    if privilege is None:
        return error_response("Пользователь не найден", 404)
    tickets_info = tickets_service.get_user_tickets(x_user_name)
    tickets = []
    for tick in tickets_info:
        tickets.append(map_ticket_to_ticket_response(tick))
    return tickets


@app.get("/me")
def get_user(x_user_name: str = Header()) -> UserInfoResponse | ErrorResponse:
    try:
        privilege = privileges_service.get_user_privelge(x_user_name)
        if privilege is None:
            return error_response("Пользователь не найден", 404)
    except CircuitOpenException:
        privilege = None
    try:
        tickets_info = tickets_service.get_user_tickets(x_user_name)
        tickets = []
        for tick in tickets_info:
            tickets.append(map_ticket_to_ticket_response(tick))
    except CircuitOpenException:
        tickets = []
    if privilege is None:
        return UserInfoResponse(tickets=tickets, privilege="")
    return UserInfoResponse(
        tickets=tickets,
        privilege=PrivilegeShortInfo(
            balance=privilege.balance, status=privilege.status
        ),
    )


@app.get("/tickets/{ticket_uid}")
def get_ticket(
    ticket_uid: uuid.UUID, x_user_name: str = Header()
) -> TicketResponse | ErrorResponse:
    ticket = tickets_service.get_ticket(ticket_uid)
    if ticket is None:
        return error_response("Билет не найден", 404)
    if ticket.username != x_user_name:
        return error_response("Билет не пренадлежит пользователю", 403)
    flight = flights_service.get_flight_by_number_or_default(ticket.flight_number)
    if flight is None:
        return error_response("Перелет не найден", 404)

    resp = TicketResponse(
        ticketUid=ticket.ticket_uid,
        flightNumber=ticket.flight_number,
        fromAirport=flight.fromAirport,
        toAirport=flight.toAirport,
        date=flight.date,
        price=ticket.price,
        status=ticket.status,
    )
    return resp


@app.post("/tickets")
def buy_ticket(
    body: TicketPurchaseRequest, x_user_name: str = Header()
) -> TicketPurchaseResponse | ValidationErrorResponse:
    flight = flights_service.get_flight_by_number(body.flightNumber)
    if flight is None:
        return ValidationErrorResponse(message="Ошибка валидации данных", errors=[])

    priv = privileges_service.get_user_privelge(x_user_name)
    if priv is None:
        return ValidationErrorResponse(message="Пользователь не существует", errors=[])

    now = datetime.now()
    ticket_uid = uuid.uuid4()

    paid_by_money = flight.price
    paid_by_bonus = 0
    if body.paidFromBalance:
        money = min(priv.balance, flight.price)
        paid_by_bonus = money
        paid_by_money = flight.price - paid_by_bonus
        if paid_by_bonus:
            privileges_service.add_transaction(
                x_user_name,
                AddTranscationRequest(
                    privilege_id=priv.id,
                    ticket_uid=ticket_uid,
                    datetime=now,
                    balance_diff=paid_by_bonus,
                    operation_type="DEBIT_THE_ACCOUNT",
                ),
            )
    else:
        privileges_service.add_transaction(
            x_user_name,
            AddTranscationRequest(
                privilege_id=priv.id,
                ticket_uid=ticket_uid,
                datetime=now,
                balance_diff=paid_by_money // 10,
                operation_type="FILL_IN_BALANCE",
            ),
        )

    priv = privileges_service.get_user_privelge(x_user_name)
    tickets_service.create_ticket(
        ticket_uid, x_user_name, flight.flightNumber, paid_by_money
    )
    return TicketPurchaseResponse(
        ticketUid=ticket_uid,
        flightNumber=body.flightNumber,
        fromAirport=flight.fromAirport,
        toAirport=flight.toAirport,
        date=now,
        price=flight.price,
        paidByMoney=paid_by_money,
        paidByBonuses=paid_by_bonus,
        status="PAID",
        privilege=PrivilegeShortInfo(balance=priv.balance, status=priv.status),
    )


def delete_with_retry(
    x_user_name, ticket_uid, max_seconds: int = 10, interval: int = 1
):
    deadline = time.time() + max_seconds
    while time.time() < deadline:
        try:
            if privileges_service.get_user_privelge_transaction(
                x_user_name, ticket_uid
            ):
                privileges_service.rollback_transaction(x_user_name, ticket_uid)
                print("deleted", time.time())
                break
        except CircuitOpenException:
            time.sleep(interval)


@app.delete("/tickets/{ticket_uid}", status_code=204)
def return_ticket(
    ticket_uid: uuid.UUID,
    background_tasks: BackgroundTasks,
    x_user_name: str = Header(),
):
    ticket = tickets_service.get_ticket(ticket_uid)
    if ticket is None:
        return error_response("Билет не существует", 404)
    if ticket.username != x_user_name:
        return error_response("Билет не принадлежит пользователю", 403)
    if ticket.status != "PAID":
        return error_response("Билет не может быть отменен", 400)
    tickets_service.delete_ticket(ticket_uid)

    background_tasks.add_task(lambda: delete_with_retry(x_user_name, ticket_uid))


@app.get("/privilege")
def get_privilege(x_user_name: str = Header()) -> PrivilegeInfoResponse:
    a = privileges_service.get_user_privelge(x_user_name)
    if a is None:
        return error_response("Пользователь не сущесвует", 404)
    b = privileges_service.get_user_privelge_history(x_user_name)
    his = []
    for it in b:
        his.append(
            BalanceHistory(
                date=it.datetime,
                ticketUid=it.ticket_uid,
                balanceDiff=it.balance_diff,
                operationType=it.operation_type,
            )
        )
    return PrivilegeInfoResponse(balance=a.balance, status=a.status, history=his)


@app.get("/manage/health", status_code=201)
def health():
    pass
