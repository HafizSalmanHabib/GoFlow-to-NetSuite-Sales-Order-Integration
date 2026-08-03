/**
 * @NApiVersion 2.1
 * @NScriptType MapReduceScript
 *
 * GoFlow -> NetSuite Sales Order poller
 *
 * GoFlow's API has no outbound webhook, so this script pulls new orders on
 * a schedule instead of waiting for a push. Deploy as a Scheduled Script
 * (Map/Reduce is used here for built-in pagination/parallelism handling,
 * but a plain Scheduled Script would also work at low order volume).
 *
 * Flow per run:
 *   1. getInputData - call GET /v1/orders on GoFlow, filtered to orders not
 *      yet marked as sent to accounting. Follows the `next` pagination cursor.
 *   2. map - for each order: resolve customer (via store mapping), resolve
 *      subsidiary, resolve items, create the Sales Order, then write the
 *      resulting order number back to GoFlow via PUT .../invoice-number.
 *   3. summarize - log failures for review.
 *
 * OPEN QUESTIONS TO CONFIRM WITH THE CLIENT BEFORE GOING LIVE:
 *   - Should GoFlow `store` map 1:1 to a NetSuite customer (wholesale
 *     account model), or does `shipping_address` need to drive customer
 *     creation instead (B2C model)? Coded here for the store-as-customer
 *     model, since Health Garden is a distribution business.
 *   - Which GoFlow order statuses should actually generate a Sales Order?
 *     (e.g. probably not "draft", "canceled", "on_hold")
 *   - Is the `charges[type=price].amount` on each line a unit price or an
 *     extended line total? This script treats it as the extended total and
 *     sets NetSuite's line `amount` field directly to avoid the ambiguity -
 *     confirm against a real order before trusting this in production.
 */

define(['N/https', 'N/record', 'N/search', 'N/log', 'N/runtime'],
  function (https, record, search, log, runtime) {

    // --- Configuration -----------------------------------------------------

    var GOFLOW_SUBDOMAIN = 'yoursubdomain'; // e.g. https://yoursubdomain.api.goflow.com
    var GOFLOW_BASE_URL = 'https://' + GOFLOW_SUBDOMAIN + '.api.goflow.com/v1';

    // Statuses that should generate a NetSuite Sales Order. Adjust with the client.
    var ELIGIBLE_STATUSES = ['ready_to_fulfill', 'ready_to_pick', 'shipped'];

    // Map GoFlow store id -> NetSuite customer internal id.
    // Populate this from a lookup/custom record in production rather than
    // hardcoding, once the store-vs-customer model is confirmed.
    var STORE_TO_CUSTOMER_MAP = {
      // '1001': '245',   // GoFlow store id -> NetSuite customer internal id
    };

    // Map GoFlow store id -> NetSuite subsidiary internal id.
    var STORE_TO_SUBSIDIARY_MAP = {
      // '1001': '2',
    };

    var EXTERNAL_ID_FIELD = 'custbody_goflow_order_id'; // create this NetSuite body field first

    // Store the token in a script parameter (checked "Secure" if using a
    // credential field) rather than hardcoding it.
    function getGoflowToken() {
      return runtime.getCurrentScript().getParameter({ name: 'custscript_goflow_api_token' });
    }

    // -----------------------------------------------------------------------

    function getInputData() {
      var orders = [];
      var url = GOFLOW_BASE_URL + '/orders' +
        '?filters[external_transactions.accounting_invoice.sent_at:exists]=false';

      ELIGIBLE_STATUSES.forEach(function (status) {
        url += '&filters[status]=' + encodeURIComponent(status);
      });

      var nextUrl = url;
      var pageGuard = 0;

      while (nextUrl && pageGuard < 50) { // pageGuard: safety cap on runaway pagination
        var response = callGoflow(nextUrl, 'GET');
        var body = JSON.parse(response.body);

        orders = orders.concat(body.data);
        nextUrl = body.next;
        pageGuard++;
      }

      log.audit('GoFlow orders fetched', orders.length);
      return orders;
    }

    function map(context) {
      var order = JSON.parse(context.value);

      try {
        var existing = findExistingOrder(order.order_number);
        if (existing) {
          log.debug('Skipping duplicate', order.order_number);
          return;
        }

        var storeId = String(order.store.id);
        var customerId = STORE_TO_CUSTOMER_MAP[storeId];
        var subsidiaryId = STORE_TO_SUBSIDIARY_MAP[storeId];

        if (!customerId || !subsidiaryId) {
          throw new Error('No customer/subsidiary mapping for GoFlow store id ' + storeId);
        }

        var soId = createSalesOrder(order, customerId, subsidiaryId);
        var soRecord = record.load({ type: record.Type.SALES_ORDER, id: soId });
        var tranId = soRecord.getValue({ fieldId: 'tranid' });

        writeInvoiceNumberBack(order.id, tranId);

        context.write({ key: order.order_number, value: 'Created SO ' + soId });

      } catch (e) {
        log.error('Failed to process GoFlow order ' + order.order_number, e);
        context.write({ key: order.order_number, value: 'ERROR: ' + e.message });
      }
    }

    function summarize(summary) {
      var errorCount = 0;
      summary.output.iterator().each(function (key, value) {
        if (String(value).indexOf('ERROR') === 0) {
          errorCount++;
          log.error('Order failed', key + ': ' + value);
        }
        return true;
      });
      log.audit('Run complete', errorCount + ' errors');

      if (summary.inputSummary.error) {
        log.error('getInputData error', summary.inputSummary.error);
      }
    }

    // --- Helpers -------------------------------------------------------------

    function callGoflow(url, method, bodyObj) {
      var headers = {
        'Authorization': 'Bearer ' + getGoflowToken(),
        'Content-Type': 'application/json',
        'X-Beta-Contact': 'yourteam@yourcompany.com' // required for beta endpoints like invoice-number
      };

      var options = { url: url, headers: headers };
      if (bodyObj) {
        options.body = JSON.stringify(bodyObj);
      }

      var response;
      if (method === 'PUT') {
        response = https.put(options);
      } else {
        response = https.get(options);
      }

      if (response.code === 429) {
        // Basic retry-after handling; production version should actually
        // sleep/requeue rather than fail outright.
        throw new Error('GoFlow rate limit hit, retry later');
      }
      if (response.code >= 400) {
        throw new Error('GoFlow API error ' + response.code + ': ' + response.body);
      }

      return response;
    }

    function findExistingOrder(orderNumber) {
      var found = null;
      search.create({
        type: search.Type.SALES_ORDER,
        filters: [[EXTERNAL_ID_FIELD, 'is', orderNumber]],
        columns: ['internalid']
      }).run().each(function (result) {
        found = result.getValue({ name: 'internalid' });
        return false;
      });
      return found;
    }

    function resolveItem(product, listing) {
      var itemKey = (product && product.item_number) || (listing && listing.sku);
      if (!itemKey) {
        throw new Error('Order line has no item_number or sku to match against');
      }

      var results = search.create({
        type: search.Type.ITEM,
        filters: [['itemid', 'is', itemKey]],
        columns: ['internalid']
      }).run().getRange({ start: 0, end: 1 });

      if (!results.length) {
        throw new Error('No NetSuite item found for: ' + itemKey);
      }
      return results[0].getValue({ name: 'internalid' });
    }

    function getLinePriceAmount(line) {
      var priceCharge = (line.charges || []).filter(function (c) { return c.type === 'price'; })[0];
      return priceCharge ? priceCharge.amount : 0;
    }

    function createSalesOrder(order, customerId, subsidiaryId) {
      var so = record.create({ type: record.Type.SALES_ORDER, isDynamic: true });

      so.setValue({ fieldId: 'entity', value: customerId });
      so.setValue({ fieldId: 'subsidiary', value: subsidiaryId });
      so.setValue({ fieldId: 'trandate', value: new Date(order.date) });
      so.setValue({ fieldId: EXTERNAL_ID_FIELD, value: order.order_number });

      if (order.purchase_order_number) {
        so.setValue({ fieldId: 'otherrefnum', value: order.purchase_order_number });
      }

      if (order.shipping_address) {
        so.setValue({ fieldId: 'shipaddress', value: formatAddress(order.shipping_address) });
      }

      order.lines.forEach(function (line) {
        var itemId = resolveItem(line.product, line.listing);
        var amount = getLinePriceAmount(line);

        so.selectNewLine({ sublistId: 'item' });
        so.setCurrentSublistValue({ sublistId: 'item', fieldId: 'item', value: itemId });
        so.setCurrentSublistValue({ sublistId: 'item', fieldId: 'quantity', value: line.quantity.amount });
        // Setting amount directly (extended line total) rather than rate,
        // to sidestep the unit-vs-total ambiguity noted above.
        so.setCurrentSublistValue({ sublistId: 'item', fieldId: 'amount', value: amount });
        so.commitLine({ sublistId: 'item' });
      });

      return so.save();
    }

    function formatAddress(addr) {
      return [addr.street1, addr.street2, addr.city, addr.state, addr.zip_code, addr.country_code]
        .filter(Boolean)
        .join(', ');
    }

    function writeInvoiceNumberBack(goflowOrderId, netsuiteTranId) {
      var url = GOFLOW_BASE_URL + '/orders/' + goflowOrderId + '/invoice-number';
      callGoflow(url, 'PUT', { invoice_number: netsuiteTranId });
    }

    return {
      getInputData: getInputData,
      map: map,
      summarize: summarize
    };
  });
