/* ************************************************************************
Copyright:
  2012 Tomek Kuprowski
License:
  GPLv2: http://www.gnu.org/licences/gpl.html
Authors:
  Tomek Kuprowski (tomekkuprowski at gmail dot com)
 ************************************************************************ */
qx.Class.define('helenos.model.QxCombinedQueryParam', {
	extend : qx.core.Object,

	construct : function() {
		this.base(arguments);
	},

	properties : {
		name : {},
		validationClass : {},
		comparisonOperator : {},
		from : {},
		to : {}
	}
});
