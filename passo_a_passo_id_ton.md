# Identificação dos clientes Ton na Mastercard

Nesse arquivo iremos detalhar o passo a passo para identificar os clientes Ton na Mastercard.


Resumo:
 - Pegar todos os ECs do autorizador
 - Achar o mmhid mais próximo, e utilizar esse mmhid



Usar diversas informações para identificar o EC mais próximo.
Variáveis 

Nome
Do autorizador





Identificação de Ton a nas Bases da Mastercard

Campos que podem ajudar a identificar

de42
de43 / name
tax_id


Rômullo Carvalho
no fim isso do Ton então vai dar pra identificar né? tem o de42 e 43 em ambas as bases


Ruan Valente Staffuzza
 Sim, a princípio vai dar. A única questão é que de 42 não temos pra todos. Isso eu ainda não vi direito, mas pelo menos lá na master ainda tem muito cara ton com PG*TON e esses caras não tem de42. Vamos torcer pra no mês que vem ter reduzido bastante esses caras *ton


Rômullo Carvalho
mas esses caras da pra fazer o join com o autorizador, pelo de43 né?! os que não possuem de42.


A princípio vai dar pra identificar os clientes Ton pelo de42 e o de43 de ambas as bases. 
A única questão é que de 42 não temos pra todos.

