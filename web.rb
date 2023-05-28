# coding: utf-8
require 'tiny_tds'

DCT = RDF::Vocabulary.new("http://purl.org/dc/terms/")
SCHEMA = RDF::Vocabulary.new("http://schema.org/")
PROV = RDF::Vocabulary.new("http://www.w3.org/ns/prov#")
CRM = RDF::Vocabulary.new("http://data.rollvolet.be/vocabularies/crm/")
PRICE = RDF::Vocabulary.new("http://data.rollvolet.be/vocabularies/pricing/")
VCARD = RDF::Vocabulary.new("http://www.w3.org/2006/vcard/ns#")
NCAL = RDF::Vocabulary.new("http://www.semanticdesktop.org/ontologies/2007/04/02/ncal#")
FOAF = RDF::Vocabulary.new("http://xmlns.com/foaf/0.1/")
FRAPO = RDF::Vocabulary.new("http://purl.org/cerif/frapo/")
PERSON = RDF::Vocabulary.new("http://www.w3.org/ns/person#")
DOSSIER = RDF::Vocabulary.new("https://data.vlaanderen.be/ns/dossier#")
P2PO_INVOICE = RDF::Vocabulary.new("https://purl.org/p2p-o/invoice#")
P2PO_DOCUMENT = RDF::Vocabulary.new("https://purl.org/p2p-o/document#")
P2PO_PRICE = RDF::Vocabulary.new("https://purl.org/p2p-o/price#")
SKOS = RDF::Vocabulary.new("http://www.w3.org/2004/02/skos/core#")
TMO = RDF::Vocabulary.new("http://www.semanticdesktop.org/ontologies/2008/05/20/tmo#")
OWL = RDF::Vocabulary.new("http://www.w3.org/2002/07/owl#")
ADMS = RDF::Vocabulary.new("http://www.w3.org/ns/adms#")

BASE_URI = 'http://data.rollvolet.be/%{resource}/%{id}'
BOOLEAN_DT = RDF::URI('http://mu.semte.ch/vocabularies/typed-literals/boolean')

OUTPUT_FOLDER = '/data'
ROLLVOLET_GRAPH = 'http://mu.semte.ch/graphs/rollvolet'

def create_sql_client
  client = TinyTds::Client.new username: 'sa', password: ENV['SQL_PASSWORD'], host: 'sql-database', database: 'Klanten'
  Mu.log.info "Connected to SQL database" if client.active?
  client
end

def write_graph(filename, graph)
  file_path = File.join(OUTPUT_FOLDER, "#{filename}.ttl")
  Mu.log.info "Writing generated data to file #{file_path}"
  RDF::Writer.open(file_path, format: :ttl) { |writer| writer << graph }
  File.open("#{OUTPUT_FOLDER}/#{filename}.graph", "w+") { |f| f.puts(ROLLVOLET_GRAPH) }
end

def write_query(filename, sparql_query)
  file_path = File.join(OUTPUT_FOLDER, "#{filename}.sparql")
  Mu.log.info "Writing SPARQL query to file #{file_path}"
  File.open(file_path, "w+") { |f| f.puts(sparql_query) }
end


require_relative 'conversions/utils'
require_relative 'conversions/codelists'
require_relative 'conversions/offerlines'
require_relative 'conversions/invoicelines'
require_relative 'conversions/telephones'
require_relative 'conversions/emails'
require_relative 'conversions/customer-identifiers'
require_relative 'conversions/calendar-events'
require_relative 'conversions/employees'
require_relative 'conversions/working-hours'
require_relative 'conversions/cases'
require_relative 'conversions/invoices'
require_relative 'conversions/offers'

post '/legacy-calculation-lines' do
  sql_client = create_sql_client()
  calculation_line_per_offerline(sql_client)
  status 204
end

post '/offerlines-to-triplestore' do
  sql_client = create_sql_client()
  offerlines_to_triplestore(sql_client)
  status 204
end

post '/invoicelines-to-triplestore' do
  sql_client = create_sql_client()
  invoicelines_to_triplestore(sql_client)
  status 204
end

post '/supplements-to-triplestore' do
  sql_client = create_sql_client()
  supplements_to_triplestore(sql_client)
  status 204
end

post '/telephones-to-triplestore' do
  sql_client = create_sql_client()
  telephones_to_triplestore(sql_client)
  status 204
end

post '/emails-to-triplestore' do
  sql_client = create_sql_client()
  emails_to_triplestore(sql_client)
  status 204
end

post '/customer-identifiers-to-triplestore' do
  sql_client = create_sql_client()
  customer_identifiers_to_triplestore(sql_client)
  status 204
end

post '/request-calendar-events-to-triplestore' do
  sql_client = create_sql_client()
  request_calendar_events_to_triplestore(sql_client)
  status 204
end

post '/intervention-calendar-events-to-triplestore' do
  sql_client = create_sql_client()
  intervention_calendar_events_to_triplestore(sql_client)
  status 204
end

post '/order-calendar-events-to-triplestore' do
  sql_client = create_sql_client()
  order_calendar_events_to_triplestore(sql_client)
  status 204
end

post '/employees-to-triplestore' do
  sql_client = create_sql_client()
  employees_to_triplestore(sql_client)
  status 204
end

post '/working-hours-to-triplestore' do
  sql_client = create_sql_client()
  working_hours_to_triplestore(sql_client)
  status 204
end

post '/cases-to-triplestore' do
  sql_client = create_sql_client()
  cases_to_triplestore(sql_client)
  status 204
end

post '/deposit-invoices-to-triplestore' do
  sql_client = create_sql_client()
  deposit_invoices_to_triplestore(sql_client)
  status 204
end

post '/invoices-to-triplestore' do
  sql_client = create_sql_client()
  invoices_to_triplestore(sql_client)
  status 204
end

post '/offers-to-triplestore' do
  sql_client = create_sql_client()
  offers_and_orders_to_triplestore(sql_client)
  status 204
end
