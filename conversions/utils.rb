def format_decimal(number)
  if number then sprintf("%0.2f", number).gsub(/(\d)(?=\d{3}+\.)/, '\1 ').gsub(/\./, ',') else '' end
end

def format_request_number(number)
  if number
    number.to_s.reverse.chars.each_slice(3).map(&:join).join(".").reverse
  else
    number
  end
end
