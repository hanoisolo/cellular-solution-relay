"""Inbound voice knowledge base for the Cellular Solution relay.

Transcribed from Bell_EPP_Voice_Agent_Knowledge.md (version 1.2), which lives
outside this repository at the project root. Keep the two in step: the .md is
the owner's source of truth, and this module is the copy the inbound system
prompt actually carries.

The text is reference material for the model, not a script. The rules that
govern how it is used — answer in one to three spoken sentences, never read it
out, never announce a transfer, never quote a price that is not verified —
live in server.SYSTEM_PROMPT, which appends this block.
"""

INBOUND_KNOWLEDGE = """# Bell EPP Voice Agent Knowledge Base
Version: 1.2
Audience: Customers calling a Bell Authorized Dealer for EPP orders, activations, upgrades, migrations, and support.
Dealer Details: Cellular Solution | Main Line: 905-264-2228 | Email: corpsales@cellularsolution.ca | Hours: Monday to Friday, 9 AM to 5 PM Eastern.

---

# Intent: Order Status

## Customer Examples

- What's the status of my order?
- Has my order been processed?
- Any update on my order?
- Was my order accepted?

## Response

Your order has been received and is being processed.

Processing depends on:
- Employment validation
- Inventory availability
- Activation requirements
- Number transfer requirements

Once complete, you will receive activation instructions and shipping information if applicable.

## Follow Up

Do you have your order number available so I can connect you with a live agent to check on it?

## Follow Up Responses

- **If Yes (provides or confirms order number):** Thank you. Please have that order number ready—I am connecting you to a live specialist at Cellular Solution now. *(Action: Transfer to 905-264-2228 during business hours Monday to Friday, 9 AM to 5 PM; if after hours, advise them to email their order number to corpsales@cellularsolution.ca).*
- **If No:** No problem at all. I can still connect you with our team so they can look up your order using your full name and email address, or you can email us anytime at corpsales@cellularsolution.ca. Would you like me to transfer you now?

---

# Intent: Employment Validation

## Customer Examples

- How do I validate my employment?
- I don't have a corporate email.
- What documents can I upload?

## Response

You can validate your employment in one of two ways:

1. Reply from your corporate work email address.

OR

2. Upload an approved proof of employment document such as:
   - Recent pay stub
   - Employment letter
   - Employee ID card

After validation, Bell will issue a verification code that must be forwarded to our team.

## Follow Up

Do you have access to your corporate work email address?

## Follow Up Responses

- **If Yes:** Great. You can validate directly on our Employee Offers portal using your work email, and Bell will send your verification code right to that inbox. Once you receive it, forward the email to corpsales@cellularsolution.ca so we can finalize your order.
- **If No:** That's completely fine. On the Employee Offers portal, choose the document upload option and submit a recent pay stub, an official employment letter, or your employee ID card—just make sure your name and employer name are clearly visible.

---

# Intent: Verification Code / Offer Code

## Customer Examples

- What is the code Bell sent me?
- Why do I need the offer code?
- Where do I find my code?

## Response

Bell sends a unique verification code after your employment has been validated.

This code confirms your eligibility for EPP pricing and must be provided before your activation or upgrade can be completed.

Please forward the code exactly as received to corpsales@cellularsolution.ca

## Follow Up

Have you already received your verification code from Bell?

## Follow Up Responses

- **If Yes:** Perfect. Please forward that email from Bell—without altering the code—to corpsales@cellularsolution.ca along with your order number if you have one, and our team will apply your EPP pricing right away.
- **If No:** Be sure to check your spam or junk folder first, as Bell's validation emails sometimes land there. If you submitted a document like a pay stub, Bell may still be reviewing it. Once it arrives, just forward it to corpsales@cellularsolution.ca.

---

# Intent: Waiting After Submitting Documents

## Customer Examples

- I uploaded my pay stub.
- What's next?
- How long does verification take?

## Response

After Bell reviews your employment validation, a verification code will be issued.

Once the code is received and confirmed, we can continue processing your order.

Processing times vary depending on Bell's validation queue.

## Follow Up

How long ago did you submit your validation documents?

## Follow Up Responses

- **If Less Than 24 Hours / Today:** Since you submitted it recently, Bell's validation team is likely still reviewing it. Keep an eye on your inbox and spam folder, and forward the code to corpsales@cellularsolution.ca as soon as it arrives.
- **If More Than 1–2 Business Days:** Since it has been a couple of business days, let's have one of our specialists check on your validation status. Would you like me to connect you with a live agent at 905-264-2228?

---

# Intent: Existing Bell Customer Moving to EPP

## Customer Examples

- Can I move my Bell account to EPP?
- Can I use my existing Bell number?

## Response

Existing Bell customers may require a migration process before joining an EPP program.

Eligibility depends on:
- Current account type
- Existing contract status
- Device financing status
- Selected EPP program

Additional migration requirements or fees may apply.

## Follow Up

Is your current mobile service already with Bell on a personal consumer account, or is it with another carrier?

## Follow Up Responses

- **If Currently With Bell:** Because you are already with Bell, moving to an EPP plan requires a consumer-to-EPP migration. I can connect you with a specialist who can check your account eligibility and any applicable migration fees. Would you like me to transfer you?
- **If With Another Carrier:** If you are with another provider like Rogers or Telus, it's even simpler—you qualify as a new EPP activation and can transfer your existing phone number over without migration fees.

---

# Intent: BYOD Eligibility

## Customer Examples

- Can I use a BYOD plan?
- Can I keep my phone?
- Can I bring my own device?

## Response

BYOD offers are generally available for:

- New activations
- Numbers being transferred from another carrier

Existing Bell numbers may require a migration or hardware upgrade instead.

There is no activation fee on a Bring Your Own Device activation: the activation fee is waived for BYOD.

## Follow Up

Are you currently with Bell or another mobile provider?

## Follow Up Responses

- **If With Another Provider (or New Number):** You are eligible for our Bring Your Own Device EPP plans. As long as your phone is unlocked, you can order a SIM card or eSIM online through our EPP portal and keep your current phone number.
- **If Currently With Bell:** Bell typically restricts standard BYOD promotions on existing Bell consumer lines, so a migration or hardware upgrade may be required. Let me connect you with one of our agents to review the best option for your number.

---

# Intent: Hardware Upgrade

## Customer Examples

- Can I upgrade my phone?
- Am I eligible for an upgrade?
- Can I keep my number?

## Response

Most hardware upgrades allow you to keep your current phone number.

A rate plan change may be required depending on Bell's current upgrade requirements.

Upgrade eligibility depends on:
- Device financing balance
- Contract status
- Account type

A hardware upgrade is not a Bring Your Own Device activation, so Bell's one-time $40 device handling fee applies. It is charged on the customer's next bill and covers fulfillment costs when a cellphone or device is ordered or purchased with wireless service. Smartwatches are exempt from the device handling fee.

## Follow Up

Are you looking to upgrade an existing Bell mobile number?

## Follow Up Responses

- **If Yes:** Great. You can check your upgrade eligibility date and any remaining device balance online through our portal, or I can connect you with a specialist at 905-264-2228 to review current EPP phone pricing and plan options for your line. Would you like to be connected?
- **If No (looking for a new line or switching carriers):** If you're starting a new line or switching from another carrier, you'll qualify for new-activation EPP device pricing rather than an upgrade. You can browse those offers directly on our Employee Offers portal.

---

# Intent: Keeping Existing Rate Plan

## Customer Examples

- Can I keep my current plan?
- Will my monthly price change?

## Response

In many cases, Bell requires a current eligible rate plan when processing a hardware upgrade.

Some legacy or grandfathered plans cannot be retained during an upgrade.

Specific eligibility is determined by Bell at the time of processing.

## Follow Up

Are you planning to upgrade your phone through Bell, or are you keeping your current device?

## Follow Up Responses

- **If Upgrading Phone:** When upgrading to a new financed device, Bell usually requires moving to an in-market EPP rate plan. If you'd like to compare your current monthly rate against current EPP plans before ordering, I can connect you with our team.
- **If Keeping Current Device:** If you aren't upgrading your phone, you do not need to change your rate plan—your existing Bell plan will continue as is.

---

# Intent: Shipping Status

## Customer Examples

- Has my phone shipped?
- When will I receive my device?
- Do you have tracking information?

## Response

In-stock devices generally ship within 48 to 72 hours after order completion.

Shipping timelines depend on:
- Inventory availability
- Model and storage selection
- Manufacturer supply

Tracking information is sent by email once the shipment has been created.

## Follow Up

Do you have your order number available so I can connect you with our team to check your tracking details?

## Follow Up Responses

- **If Yes:** Thank you. Have that order number handy—I am transferring you to a Cellular Solution specialist now so they can pull up your shipment status. *(Action: Transfer to 905-264-2228).*
- **If No:** That's okay. Be sure to check your email—including your junk folder—for a shipping confirmation email. If you'd still like an update, I can connect you with our team and they can look it up by your name and email address. Would you like me to transfer you?

---

# Intent: Inventory Availability

## Customer Examples

- Is my phone in stock?
- Why is my order delayed?
- Is my color backordered?

## Response

Inventory levels change frequently.

If your selected model or color is unavailable, the order will remain pending until inventory arrives.

Alternative models or colors may be available sooner.

## Follow Up

Which device model and storage size are you waiting for?

## Follow Up Responses

- **If Customer Names a Device:** Got it. Because stock changes throughout the day, let me connect you with one of our sales specialists who can check live warehouse availability for that model—or let you know if another color is ready to ship right away. Would you like me to transfer you?
- **If Unsure / General Inquiry:** No problem. If you'd like to check live availability or switch to an in-stock color to avoid waiting, I can connect you with our team at 905-264-2228.

---

# Intent: Change Ordered Device

## Customer Examples

- Can I switch devices?
- Can I change the color?
- Can I choose a different model?

## Response

Device changes may be possible before the order is processed or shipped.

Availability of the requested replacement device will determine whether the change can be completed.

## Follow Up

Has your order already shipped?

## Follow Up Responses

- **If No (or Not Yet Shipped):** Since it hasn't shipped yet, our team can check if we can modify the order or cancel and re-submit it for the device you want. Let me connect you with a specialist right away so we can catch it before dispatch. *(Action: Transfer to 905-264-2228).*
- **If Yes (Already Shipped):** Once an order has shipped, we can't modify it in transit. However, once it arrives, you can return it unopened within 14 days—or within 14 days opened with a $50 open-box fee—and place a new order for your preferred model.

---

# Intent: Number Transfer (Porting)

## Customer Examples

- How do I transfer my number?
- Why did I receive a port text?
- My number hasn't moved yet.

## Response

To transfer your number:

1. Keep your current service active.
2. Wait for the transfer authorization text message.
3. Reply YES when prompted.
4. Bell will complete the transfer once authorization is received.

If you missed the message, Bell can resend the authorization request.

## Follow Up

Have you already received the transfer authorization text message on your old SIM card?

## Follow Up Responses

- **If Yes:** Make sure your old SIM card is still in your phone and reply YES to that message right away—typically within 90 minutes of receiving it. Once you reply, your old service will stop working, which means you can insert your new Bell SIM or activate your Bell eSIM.
- **If No:** Make sure your previous carrier's SIM card is still active in your phone so it can receive SMS messages. If it's been more than a few hours since your Bell order was activated, let me connect you with our team so we can re-trigger the port request for you.

---

# Intent: Temporary Number

## Customer Examples

- Why do I have a temporary number?
- Is this my permanent number?

## Response

Temporary numbers are assigned during activation so your new Bell line can be provisioned right away.

Once your number transfer is completed, the temporary number will automatically be replaced by your permanent number.

## Follow Up

Are you currently waiting for your existing phone number to finish transferring over from another carrier?

## Follow Up Responses

- **If Yes:** That explains it—the temporary number is just a placeholder until you reply YES to your previous carrier's authorization text and the port finishes. Once complete, restart your phone and your permanent number will take over automatically.
- **If No (requested a brand-new number):** If you didn't request a number transfer and expected a permanent new number, let me connect you with a specialist at 905-264-2228 so we can verify your activation details.

---

# Intent: eSIM Setup

## Customer Examples

- How do I activate my eSIM?
- I received my eSIM.
- My QR code isn't working.

## Response

To activate your eSIM:

1. Connect to Wi-Fi.
2. Follow the activation instructions provided in your activation email.
3. Complete any required number transfer authorization requests.
4. Restart your device if prompted.

## Follow Up

Are you activating a brand-new phone number, or transferring an existing number from another carrier?

## Follow Up Responses

- **If Activating a New Number:** You can install your Bell eSIM right away. Just connect to Wi-Fi, turn off or remove your old SIM card, scan the Bell QR code from your activation email, and restart your phone.
- **If Transferring an Existing Number:** Important tip: keep your old SIM active first until you receive and reply YES to the transfer authorization text message. Once your old line loses signal, connect to Wi-Fi, turn off the old SIM, and install your new Bell eSIM.

---

# Intent: MyBell Account Setup

## Customer Examples

- How do I create a Bell account?
- How do I log into MyBell?

## Response

You can create your account using:

- The MyBell mobile app
- Bell.ca

Account setup may take up to 24 hours after activation before registration becomes available.

## Follow Up

Has your Bell line been active for more than 24 hours?

## Follow Up Responses

- **If Yes:** You're ready to register. Download the MyBell app or visit Bell.ca, click Register, and use your Bell mobile number or account number from your welcome email to set up your login.
- **If No:** Give it a full 24 hours from the time your SIM or eSIM first connected to the Bell network—Bell's billing system needs that window to sync before MyBell registration will recognize your number.

---

# Intent: AutoPay Setup

## Customer Examples

- How do I set up automatic payments?
- How do I get the AutoPay discount?

## Response

After activation:

1. Sign into MyBell.
2. Select Billing & Payments.
3. Add your preferred payment method.
4. Enable pre-authorized payments.

Promotional credits may require AutoPay enrollment within a specific timeframe.

## Follow Up

Have you already registered and logged into your MyBell account?

## Follow Up Responses

- **If Yes:** Great—just open MyBell, go to Billing & Payments, and link a bank account or eligible credit card for pre-authorized payments so any monthly AutoPay discount kicks in automatically.
- **If No:** You'll need to register for MyBell first at Bell.ca or through the MyBell app—which becomes available within 24 hours of activation—and then enable pre-authorized payments under Billing & Payments.

---

# Intent: Add an Additional Line

## Customer Examples

- Can I add another line?
- I need a second phone number.
- Can I add a family member?

## Response

Additional lines may be available depending on your EPP program's eligibility requirements.

Available pricing and options vary by employer and account type.

## Follow Up

Is the additional line for yourself or for a family member?

## Follow Up Responses

- **If For Yourself:** Depending on your employer's EPP rules, you may be able to add a second line under your profile. You can check available multi-line options on our Employee Offers portal, or I can connect you with a specialist to check your employer's limit.
- **If For a Family Member:** Many Bell EPP programs allow family add-on lines under the primary employee's account. Let me connect you with a Cellular Solution agent at 905-264-2228 so they can confirm your employer's family-line eligibility and current pricing.

---

# Intent: Additional Data Plans

## Customer Examples

- What plans are available?
- Can I add more data?
- What are my options?

## Response

Current available plans depend on:

- Employer program
- Account type
- Activation type
- Upgrade status

Only currently available Bell EPP offers can be selected.

## Follow Up

Are you shopping for a new EPP plan today, or looking to change the data plan on an existing Bell line?

## Follow Up Responses

- **If Shopping for a New Plan:** You can view all current Bell EPP plans for your employer directly on our Employee Offers portal at portal.cellularsolution.ca/epp, or I can connect you with our sales team to go over current Canada-US and data-bucket offers.
- **If Changing an Existing Line:** Plan changes on an already-active Bell line can be managed directly inside your MyBell app, or through Bell Customer Care if you want to explore in-market EPP rate plans.

---

# Intent: Cancel Order

## Customer Examples

- I want to cancel.
- Stop my order.
- I changed my mind.

## Response

Orders may typically be cancelled before shipment or activation.

Once cancelled, processing will stop and no further action will occur on the order.

## Follow Up

Do you have your order number available so I can connect you with our team right away?

## Follow Up Responses

- **If Yes:** Please keep that order number ready—I am transferring you to a live agent now so we can request cancellation before your order ships or activates. *(Action: Transfer to 905-264-2228 during business hours; if after hours, instruct them to immediately email corpsales@cellularsolution.ca with "URGENT CANCEL" and their order number in the subject line).*
- **If No:** That's fine. Let me connect you with a live agent right away so they can look up your order by name and email and stop it before shipment.

---

# Intent: Return Policy

## Customer Examples

- Can I return my phone?
- I don't like the device.
- What is the buyer's remorse period?

## Response

Eligible activations may qualify for Bell's buyer's remorse policy, allowing returns within 14 days. A one-time $50 fee applies to open-box returns.

Return eligibility also depends on:
- Device condition (must be like-new with all original packaging and accessories)
- Usage limits
- Bell return requirements

Please contact our team as soon as possible after activation.

## Follow Up

Was your device activated within the last 14 days?

## Follow Up Responses

- **If Yes (Within 14 Days):** Since you're within the 14-day window, let's get you connected with a specialist right away to start the return authorization—just make sure you keep the original box, cables, and manuals in like-new condition. *(Action: Transfer to 905-264-2228).*
- **If No (Over 14 Days):** Because it has been more than 14 days since activation, the order falls outside the standard buyer's remorse return period. However, I can still connect you with a specialist to review your account options if you'd like.

---

# Intent: Activation Date

## Customer Examples

- Can I activate later?
- When will my line become active?
- Can I choose an activation date?

## Response

Requested activation dates are generally processed according to the information provided during ordering.

Actual activation timing depends on validation, inventory, and transfer requirements.

## Follow Up

Have you already placed your order, or are you planning to submit one soon?

## Follow Up Responses

- **If Already Placed:** If you already placed your order and need to adjust your activation timing, let me connect you with our team at 905-264-2228 so they can check whether your line has already been provisioned.
- **If Placing Soon:** When you place your order online, you can note your preferred timing, or reach out to our team right after submitting so we can coordinate activation around your current billing cycle.

---

# Intent: Existing Bell Number on Consumer Plan

## Customer Examples

- I already have Bell.
- Can I move my number to Corporate EPP?
- Why isn't my Bell number eligible?

## Response

Existing Bell numbers often require migration before becoming eligible for Corporate EPP programs.

In some cases:
- Migration fees apply.
- Rate plan restrictions apply.
- Hardware upgrades may be required.

Eligibility is determined by Bell's program rules.

## Follow Up

Are you currently under a device financing contract with Bell, or is your phone paid off?

## Follow Up Responses

- **If Phone Is Paid Off (Month-to-Month):** Being month-to-month gives you much more flexibility to migrate onto an EPP plan. Let me connect you with one of our specialists to confirm your employer's migration rules and any one-time migration fee.
- **If Under Contract / Financing:** If you still have a remaining device balance, Bell may require paying off that balance or upgrading your device as part of the migration to EPP. Would you like me to connect you with a specialist to check your exact options?

---

# Intent: Add BYOD Line to Existing Account

## Customer Examples

- I only need a SIM card.
- Can I add another BYOD line?
- I want another phone number.

## Response

Additional BYOD lines may be available depending on your EPP program eligibility.

The available plans and pricing depend on your employer program and account structure.

## Follow Up

Will this additional BYOD line be for a brand-new phone number, or are you transferring a number from another carrier?

## Follow Up Responses

- **If New Number:** Great—you can order a Bring Your Own Device SIM or eSIM directly through our Employee Offers portal, and we'll activate a new number under your EPP program.
- **If Transferring From Another Carrier:** You can order a BYOD SIM or eSIM on our portal and enter the number you're transferring during checkout—just keep your old SIM active until you approve the transfer text message.

---

# Escalation Rules

Escalate to Human Agent (905-264-2228) If:

- Customer requests billing adjustments
- Customer disputes charges
- Customer requests credits
- Customer requests contract exceptions
- Customer reports fraud
- Customer requests account ownership changes
- Customer requests business-account modifications
- Customer requests unavailable promotional pricing
- Customer has a shipping issue requiring investigation

---

# Unsupported Requests

If asked:

- Why did Bell increase my bill?
- Can you give me a credit?
- Can you waive a charge?
- Can you remove roaming fees?
- Can you see my Bell account?

Response:

As a Bell Authorized Dealer, we do not have access to Bell billing systems or account-level billing information. Please contact Bell Customer Care directly by dialing 611 from your Bell phone or 1-800-667-0123 for assistance with billing-related requests.

---

# Standard Closing

Is there anything else I can help you with regarding your order, activation, upgrade, or EPP services today?
"""
