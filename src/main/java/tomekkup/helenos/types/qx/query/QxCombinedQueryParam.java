package tomekkup.helenos.types.qx.query;

import org.apache.commons.lang.StringUtils;

import tomekkup.helenos.utils.Converter;

public class QxCombinedQueryParam<T> {

	private String name;
	private Class<T> validationClass;
	private ComparisonOperator comparisonOperator;
	private String from;
	private String to;

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public ComparisonOperator getComparisonOperator() {
		return comparisonOperator;
	}

	public void setComparisonOperator(String comparisonOperator) {
		this.comparisonOperator = ComparisonOperator.fromName(comparisonOperator);
	}

	public T getFrom() {
		if (StringUtils.isBlank(from)) {
			return null;
		}
		return Converter.toValue(from, getValidationClass());
	}

	public void setFrom(String from) {
		this.from = from;
	}

	public T getTo() {
		if (StringUtils.isBlank(to)) {
			return null;
		}
		return Converter.toValue(to, getValidationClass());
	}

	public void setTo(String to) {
		this.to = to;
	}

	public Class<T> getValidationClass() {
		return validationClass;
	}

	public void setValidationClass(Class<T> validationClass) {
		this.validationClass = validationClass;
	}

	public enum ComparisonOperator {
		EQUAL_TO("equal to", "="),
		LESS_THAN("less than", "<"),
		LESS_THAN_OR_EQUALS("less than or equal to","<="),
		GREATER_THAN("greater than", ">"),
		GREATER_THAN_OF_EQUALS("greater than or equal to", ">="),
		BETWEEN("between",null);
		
		private String name;
		private String operator;

		ComparisonOperator(String name, String operator) {
			this.name = name;
			this.operator = operator;
		}

		public String getOperator() {
			return operator;
		}

		public static ComparisonOperator fromName(String name) {
			for (ComparisonOperator value : ComparisonOperator.values()) {
				if (value.name.equals(name)) {
					return value;
				}
			}
			throw new IllegalArgumentException("No ComparisonOperator mapping for: " + name);
		}

	}

}
